<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\SocketStrategy\Windows;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Queue;
use Amp\Socket\BindContext;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\Socket\SocketAddress;
use Amp\Socket\SocketException;
use Amp\Sync\Channel;
use IfCastle\AmpPool\EventWeakHandler;
use IfCastle\AmpPool\Strategies\SocketStrategy\Windows\Messages\MessageSocketTransfer;
use IfCastle\AmpPool\WorkerEventEmitterInterface;

final class ServerSocketFactoryWindows implements ServerSocket
{
    use ForbidCloning;
    use ForbidSerialization;

    private readonly DeferredFuture $onClose;
    /** @var Queue<MessageSocketTransfer> */
    private readonly Queue $queue;
    private readonly ConcurrentIterator $iterator;
    private mixed $workerEventHandler;

    public function __construct(
        private readonly Channel       $writeOnlyChannel,
        private readonly SocketAddress $socketAddress,
        private readonly BindContext   $bindContext,
        private readonly WorkerEventEmitterInterface $workerEventEmitter,
        private readonly Cancellation $abortCancellation
    ) {
        $this->onClose              = new DeferredFuture();
        $this->queue                = new Queue();
        $this->iterator             = $this->queue->iterate();

        $self                       = \WeakReference::create($this);

        $this->workerEventHandler   = new EventWeakHandler(
            $this,
            static function (mixed $message, int $workerId = 0) use ($self) {
                $self->get()?->workerEventHandler($message, $workerId);
            }
        );

        $this->workerEventEmitter->addWorkerEventListener($this->workerEventHandler);
    }

    public function close(): void
    {
        if (false === $this->queue->isComplete()) {
            $this->queue->complete();
        }

        if (false === $this->onClose->isComplete()) {
            $this->onClose->complete();
        }

        if ($this->workerEventHandler !== null) {
            $this->workerEventEmitter->removeWorkerEventListener($this->workerEventHandler);
            $this->workerEventHandler = null;
        }
    }

    public function isClosed(): bool
    {
        return $this->onClose->isComplete();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->onClose->getFuture()->finally($onClose);
    }

    /**
     * @throws SocketException
     */
    public function accept(?Cancellation $cancellation = null): ?Socket
    {
        if ($cancellation !== null) {
            $cancellation           = new CompositeCancellation($cancellation, $this->abortCancellation);
        } else {
            $cancellation           = $this->abortCancellation;
        }

        try {
            if (false === $this->iterator->continue($cancellation)) {
                return null;
            }
        } catch (CancelledException) {
            return null;
        }

        $message                    = $this->iterator->getValue();

        if (false === $message instanceof MessageSocketTransfer) {
            throw new SocketException('Invalid message received. Required MessageSocketTransfer. Got: '.\get_debug_type($message));
        }

        $socket                     = \socket_wsaprotocol_info_import($message->socketId);

        if (false === $socket) {
            throw new SocketException('Failed importing socket from ID: '.$message->socketId);
        }

        \socket_wsaprotocol_info_release($message->socketId);

        return ResourceSocket::fromServerSocket(\socket_export_stream($socket), $this->writeOnlyChannel, $message->socketId, $this->abortCancellation);
    }

    public function getAddress(): SocketAddress
    {
        return $this->socketAddress;
    }

    public function getBindContext(): BindContext
    {
        return $this->bindContext;
    }

    /**
     * Handle message MessageSocketTransfer to worker.
     *
     *
     */
    private function workerEventHandler(mixed $message, int $workerId = 0): void
    {
        if ($message instanceof MessageSocketTransfer) {
            $this->queue->pushAsync($message)->ignore();
        }
    }
}
