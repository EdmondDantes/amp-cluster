<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\SocketStrategy\Unix;

use Amp\ByteStream\ResourceStream;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\Parallel\Ipc\IpcHub;
use Amp\Socket\ResourceSocket;
use Amp\Socket\Socket;
use Amp\Socket\SocketException;
use Amp\TimeoutCancellation;
use Revolt\EventLoop;
use const Amp\Process\IS_WINDOWS;

final class SocketProvider
{
    private ServerSocketPipeProvider|null $provider = null;
    private Cancellation $cancellation;
    private DeferredCancellation $deferredCancellation;

    public function __construct(
        private readonly int    $workerId,
        private readonly IpcHub $hub,
        private readonly string $ipcKey,
        Cancellation            $cancellation,
        private readonly int    $timeout = 5
    ) {
        if (IS_WINDOWS) {
            throw new \Error(__CLASS__.' can\'t be used under Windows OS');
        }

        $this->provider             = new ServerSocketPipeProvider($this->workerId);
        $this->deferredCancellation = new DeferredCancellation();
        $this->cancellation         = new CompositeCancellation($cancellation, $this->deferredCancellation->getCancellation());
    }

    public function start(): void
    {
        $self                       = \WeakReference::create($this);

        EventLoop::queue(static function () use ($self) {

            $provider               = $self->get()?->provider;

            if ($provider === null) {
                return;
            }

            try {
                $provider->provideFor($self->get()->createSocketTransport(), $self->get()->cancellation);
            } catch (SocketException $exception) {

                $deferredCancellation = $self->get()?->deferredCancellation;

                // Stop the service
                if ($deferredCancellation instanceof DeferredCancellation && false === $deferredCancellation->isCancelled()) {
                    $deferredCancellation->cancel($exception);
                }

            } catch (CancelledException) {
                // Ignore
            }
        });
    }

    public function stop(): void
    {
        if (false === $this->deferredCancellation->isCancelled()) {
            $this->deferredCancellation->cancel();
        }

        $this->provider             = null;
    }

    private function createSocketTransport(): ResourceSocket
    {
        $socket                     = $this->hub->accept(
            $this->ipcKey,
            new CompositeCancellation(
                $this->cancellation,
                new TimeoutCancellation(
                    $this->timeout,
                    'Timeout while attempting to create a channel for socket transmission between processes.'
                )
            )
        );

        if (false === $socket instanceof ResourceSocket) {
            throw new \TypeError(\sprintf(
                'The %s instance returned from %s::accept() must also implement %s',
                Socket::class,
                \get_class($this->hub),
                ResourceStream::class,
            ));
        }

        return $socket;
    }
}
