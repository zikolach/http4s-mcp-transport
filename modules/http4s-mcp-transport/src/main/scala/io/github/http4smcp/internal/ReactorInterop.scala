package io.github.http4smcp.internal

import cats.effect.IO
import cats.effect.std.Queue
import cats.effect.unsafe.IORuntime
import fs2.Stream
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success

private[http4smcp] object ReactorInterop {
  // Reactor and cats-effect meet only in this file.
  // The bridge subscribes to Reactor sources and completes IO callbacks. It does not call block().
  def monoToIO[A](mono: Mono[A]): IO[A] =
    IO.async[A] { callback =>
      IO {
        val completed  = new AtomicBoolean(false)
        val disposable = mono.subscribe(
          value => if (completed.compareAndSet(false, true)) callback(Right(value)),
          error => if (completed.compareAndSet(false, true)) callback(Left(error)),
          () => if (completed.compareAndSet(false, true)) callback(Right(null.asInstanceOf[A]))
        )
        Some(IO {
          if (completed.compareAndSet(false, true)) disposable.dispose()
        })
      }
    }

  def ioUnitToMono(io: IO[Unit])(implicit runtime: IORuntime): Mono[Void] =
    Mono.create[Void] { sink =>
      val completed        = new AtomicBoolean(false)
      val (future, cancel) = io.unsafeToFutureCancelable()
      val cancelOnce       = new Disposable {
        override def dispose(): Unit =
          if (completed.compareAndSet(false, true)) {
            cancel()
            ()
          }

        override def isDisposed: Boolean = completed.get()
      }
      sink.onCancel(cancelOnce)
      sink.onDispose(cancelOnce)
      future.onComplete {
        case Success(_)     => if (completed.compareAndSet(false, true)) sink.success()
        case Failure(error) => if (completed.compareAndSet(false, true)) sink.error(error)
      }(ExecutionContext.parasitic)
      ()
    }

  def fluxToStream[A](flux: Flux[A])(implicit runtime: IORuntime): Stream[IO, A] =
    Stream
      .eval(Queue.unbounded[IO, Option[Either[Throwable, A]]])
      .flatMap { queue =>
        Stream
          .bracket {
            IO {
              flux.subscribe(
                value => queue.offer(Some(Right(value))).unsafeRunAndForget(),
                error => queue.offer(Some(Left(error))).unsafeRunAndForget(),
                () => queue.offer(None).unsafeRunAndForget()
              )
            }
          } { (disposable: Disposable) => IO(disposable.dispose()) }
          .flatMap { _ =>
            Stream
              .repeatEval(queue.take)
              .unNoneTerminate
              .flatMap {
                case Right(value) => Stream.emit(value)
                case Left(error)  => Stream.raiseError[IO](error)
              }
          }
      }
}
