<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\SocketStrategy\Windows;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\Socket\BindContext;
use Amp\Socket\ResourceServerSocket;
use Amp\Socket\ResourceSocket;
use Amp\Socket\ServerSocket;
use Amp\Socket\Socket;
use Amp\Socket\SocketAddress;
use IfCastle\AmpPool\EventWeakHandler;
use IfCastle\AmpPool\Internal\Safe;
use IfCastle\AmpPool\Strategies\SocketStrategy\Windows\Messages\MessageReady;
use IfCastle\AmpPool\Strategies\SocketStrategy\Windows\Messages\MessageSocketFree;
use IfCastle\AmpPool\Strategies\SocketStrategy\Windows\Messages\MessageSocketTransfer;
use IfCastle\AmpPool\WatcherEvents\WorkerProcessTerminating;
use IfCastle\AmpPool\WorkerGroupInterface;
use IfCastle\AmpPool\WorkerPoolInterface;
use Revolt\EventLoop;

/**
 * The class listens to the specified address
 * and calls a callback method to transmit the socket when a new packet is received.
 *
 * This class is executed inside the Watcher process.
 *
 * @internal
 */
final class SocketClientListenerProvider
{
    /**
     * List of workers that bind to current SocketAddress.
     * @var int[]
     */
    private array                   $workers          = [];
    /**
     * Map of worker statuses, where key is the worker ID and value is the status.
     * True means worker is ready to accept new socket.
     * @var array<int, bool>
     */
    private array                   $workerStatus     = [];
    /**
     * Map of transferred sockets, where key is the socket ID and value is the socket.
     * @var array<int, ResourceSocket|Socket>
     */
    private array                   $transferredSockets = [];
    private array                   $transferredSocketsByWorker = [];
    /**
     * Map of requests by worker, where key is the worker ID and value is the number of active requests.
     * @var array<int, int>
     */
    private array                   $requestsByWorker   = [];
    private Cancellation|null       $cancellation       = null;
    private DeferredCancellation    $deferredCancellation;

    public function __construct(
        private readonly SocketAddress $address,
        private readonly WorkerPoolInterface $workerPool,
        private readonly WorkerGroupInterface $workerGroup
    ) {
        $this->deferredCancellation = new DeferredCancellation;
        $this->cancellation         = new CompositeCancellation($this->workerPool->getMainCancellation(), $this->deferredCancellation->getCancellation());

        $self                       = \WeakReference::create($this);

        $this->workerPool->getWorkerEventEmitter()->addWorkerEventListener(new EventWeakHandler(
            $this,
            static function (mixed $event, int $workerId = 0) use ($self) {
                $self->get()?->eventListener($event, $workerId);
            }
        ));
    }

    public function getSocketAddress(): SocketAddress
    {
        return $this->address;
    }

    public function getWorkers(): array
    {
        return $this->workers;
    }

    public function addWorker(int $workerId): self
    {
        if (\in_array($workerId, $this->workers)) {
            return $this;
        }

        $this->workers[]            = $workerId;
        $this->workerStatus[$workerId] = true;

        return $this;
    }

    public function removeWorker(int $workerId): self
    {
        $this->shutdownWorker($workerId);

        return $this;
    }

    public function startListen(): void
    {
        EventLoop::queue($this->receiveLoop(...), $this->listenAddress($this->address));
    }

    public function stopIfNoWorkers(): bool
    {
        if (!empty($this->workers)) {
            return false;
        }

        if (false === $this->deferredCancellation->isCancelled()) {
            $this->deferredCancellation->cancel();
        }

        return true;
    }

    private function receiveLoop(ServerSocket $server): void
    {
        try {
            while (($socket = $server->accept($this->cancellation)) !== null) {

                // Select free worker
                $foundedWorkerId    = $this->pickupWorker();

                if ($foundedWorkerId === null) {
                    $socket->close();
                    return;
                }

                $foundedWorker      = $this->workerPool->findWorkerContext($foundedWorkerId);

                if ($foundedWorker === null) {
                    $socket->close();
                    return;
                }

                $pid                = $foundedWorker->getPid();

                $socketId           = Safe::execute(fn () => \socket_wsaprotocol_info_export(\socket_import_stream($socket->getResource()), $pid));

                if (false === $socketId) {
                    $socket->close();
                    throw new \Exception('Failed to export socket information');
                }

                try {
                    $foundedWorker->send(new MessageSocketTransfer($socketId));
                    $this->addTransferredSocket($foundedWorkerId, $socketId, $socket);
                } catch (\Throwable $exception) {
                    $socket->close();
                    throw $exception;
                }
            }
        } catch (CancelledException) {
            // ignore
        } finally {
            if (false === $server->isClosed()) {
                $server->close();
            }
        }
    }

    private function listenAddress(SocketAddress $address, ?BindContext $bindContext = null): ServerSocket
    {
        $bindContext                = $bindContext ?? new BindContext();

        return new ResourceServerSocket(
            $this->bind((string) $address, $bindContext),
            $bindContext
        );
    }

    private function bind(string $uri, BindContext $bindContext)
    {
        static $errorHandler;

        $context                    = \stream_context_create(\array_merge(
            $bindContext->toStreamContextArray(),
            [
                 'socket'           => [
                     'so_reuseaddr' => \PHP_OS_FAMILY === 'Windows',
                     'ipv6_v6only'  => true,
                 ],
             ],
        ));

        // Error reporting suppressed as stream_socket_server() error is immediately checked and
        // reported with an exception.
        \set_error_handler($errorHandler ??= static fn () => true);

        try {
            // Do NOT use STREAM_SERVER_LISTEN here - we explicitly invoke \socket_listen() in our worker processes
            if (!$server = \stream_socket_server($uri, $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN, $context)) {
                throw new \RuntimeException(\sprintf(
                    'Failed binding socket on %s: [Err# %s] %s',
                    $uri,
                    $errno,
                    $errstr,
                ));
            }
        } finally {
            \restore_error_handler();
        }

        return $server;
    }

    private function pickupWorker(): int|null
    {
        if (empty($this->workers)) {
            return null;
        }

        $workerId                   = $this->pickupWorkerByRequests();

        if ($workerId !== null) {
            return $workerId;
        }

        // Try to scale a worker group
        $this->workerGroup->getScalingStrategy()?->requestScaling();

        // Select random worker
        $workers                    = [];

        foreach ($this->workers as $workerId) {
            if ($this->workerPool->isWorkerRunning($workerId)) {
                $workers[]          = $workerId;
            }
        }

        return $this->workers[\array_rand($workers)];
    }

    private function pickupWorkerByRequests(): int|null
    {
        $minRequests                = 0;
        $selectedWorkerId           = null;

        $workers                    = [];

        foreach ($this->workers as $workerId) {
            if ($this->workerPool->isWorkerRunning($workerId)) {
                $workers[]          = $workerId;
            }
        }

        foreach ($workers as $workerId) {

            if (empty($this->workerStatus[$workerId])) {
                continue;
            }

            if (false === \array_key_exists($workerId, $this->requestsByWorker) || $this->requestsByWorker[$workerId] === 0) {
                return $workerId;
            }

            if ($minRequests === 0 || $this->requestsByWorker[$workerId] < $minRequests) {
                $minRequests        = $this->requestsByWorker[$workerId];
                $selectedWorkerId   = $workerId;
            }
        }

        return $selectedWorkerId;
    }

    private function eventListener(mixed $event, int $workerId = 0): void
    {
        if ($event instanceof MessageReady || $event instanceof MessageSocketTransfer) {
            $this->workerStatus[$workerId] = true;
            return;
        }

        if ($event instanceof MessageSocketFree) {
            $this->freeTransferredSocket($workerId, $event->socketId);
        }

        if ($event instanceof WorkerProcessTerminating) {
            $this->shutdownWorker($workerId);
        }
    }

    private function addTransferredSocket(int $workerId, string $socketId, ResourceSocket|Socket $socket): void
    {
        $this->transferredSockets[$socketId] = $socket;
        $this->requestsByWorker[$workerId]   = ($this->requestsByWorker[$workerId] ?? 0) + 1;
        $this->transferredSocketsByWorker[$workerId][] = $socketId;

        $this->workerStatus[$workerId]       = false;
    }

    private function freeTransferredSocket(int $workerId, ?string $socketId = null): void
    {
        if ($socketId === null) {
            return;
        }

        $this->workerStatus[$workerId]      = true;

        if (\array_key_exists($socketId, $this->transferredSockets)) {
            $this->transferredSockets[$socketId]->close();
            unset($this->transferredSockets[$socketId]);
        }

        if (\array_key_exists($workerId, $this->transferredSocketsByWorker)) {
            $this->transferredSocketsByWorker[$workerId] = \array_diff($this->transferredSocketsByWorker[$workerId], [$socketId]);
        }

        if (\array_key_exists($workerId, $this->requestsByWorker)) {
            $this->requestsByWorker[$workerId]--;

            if ($this->requestsByWorker[$workerId] < 0) {
                $this->requestsByWorker[$workerId] = 0;
            }
        }
    }

    private function shutdownWorker(int $workerId): void
    {
        $this->workerStatus[$workerId]      = false;
        $this->workers                      = \array_diff($this->workers, [$workerId]);

        if (\array_key_exists($workerId, $this->transferredSocketsByWorker)) {
            foreach ($this->transferredSocketsByWorker[$workerId] as $socketId) {
                if (\array_key_exists($socketId, $this->transferredSockets)) {
                    $this->transferredSockets[$socketId]->close();
                    unset($this->transferredSockets[$socketId]);
                }
            }

            $this->transferredSocketsByWorker[$workerId] = [];
        }

        if (\array_key_exists($workerId, $this->requestsByWorker)) {
            $this->requestsByWorker[$workerId] = 0;
        }
    }

}
