<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\SocketStrategy\Unix;

use Amp\DeferredFuture;
use Amp\Parallel\Ipc;
use Amp\Socket\ResourceSocket;
use Amp\Socket\ServerSocketFactory;
use Amp\TimeoutCancellation;
use IfCastle\AmpPool\EventWeakHandler;
use IfCastle\AmpPool\Strategies\SocketStrategy\SocketStrategyInterface;
use IfCastle\AmpPool\Strategies\SocketStrategy\Unix\Messages\InitiateSocketTransfer;
use IfCastle\AmpPool\Strategies\SocketStrategy\Unix\Messages\SocketTransferInfo;
use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;
use function Amp\Future\await;

final class SocketUnixStrategy extends WorkerStrategyAbstract implements SocketStrategyInterface
{
    private ServerSocketPipeFactory|null $socketPipeFactory = null;
    private string              $uri                = '';
    private string              $key                = '';
    private DeferredFuture|null $deferredFuture     = null;
    private EventWeakHandler|null $workerEventHandler = null;

    /** @var SocketProvider[] */
    private array $workerSocketProviders = [];

    public function __construct(private readonly int $ipcTimeout = 5)
    {
    }

    public function onStarted(): void
    {
        $workerPool                 = $this->getWorkerPool();

        if ($workerPool !== null) {

            $self                   = \WeakReference::create($this);

            $workerPool->getWorkerEventEmitter()
                       ->addWorkerEventListener(static function (mixed $message, int $workerId = 0) use ($self) {
                           $self->get()?->handleMessage($message, $workerId);
                       });

            return;
        }

        $worker                     = $this->getSelfWorker();

        if ($worker === null) {
            return;
        }

        $this->deferredFuture       = new DeferredFuture;

        $self                       = \WeakReference::create($this);
        $this->workerEventHandler   = new EventWeakHandler(
            $this,
            static function (mixed $message, int $workerId = 0) use ($self) {
                $self->get()?->handleMessage($message, $workerId);
            }
        );

        $worker->getWorkerEventEmitter()->addWorkerEventListener($this->workerEventHandler);

        $worker->sendMessageToWatcher(
            new InitiateSocketTransfer($worker->getWorkerId(), $worker->getWorkerGroup()->getWorkerGroupId())
        );
    }

    public function onStopped(): void
    {
        if (false === $this->deferredFuture?->isComplete()) {
            $this->deferredFuture->complete();
            $this->deferredFuture   = null;
        }

        if ($this->workerEventHandler !== null) {
            $this->getSelfWorker()?->getWorkerEventEmitter()->removeWorkerEventListener($this->workerEventHandler);
            $this->workerEventHandler = null;
        }

        $this->socketPipeFactory    = null;

        $providers                  = $this->workerSocketProviders;
        $this->workerSocketProviders = [];

        foreach ($providers as $socketProvider) {
            $socketProvider->stop();
        }
    }

    /**
     * Calling this method pauses the Worker’s execution thread until the Watcher returns data for socket
     * initialization.
     *
     */
    public function getServerSocketFactory(): ServerSocketFactory|null
    {
        if ($this->socketPipeFactory !== null) {
            return $this->socketPipeFactory;
        }

        if ($this->deferredFuture === null) {
            throw new \Error('Wrong usage of the method getServerSocketFactory(). The deferredFuture undefined.');
        }

        await([$this->deferredFuture->getFuture()], new TimeoutCancellation($this->ipcTimeout, 'Timeout waiting for socketPipeFactory from the parent process'));

        return $this->socketPipeFactory;
    }

    private function createIpcForTransferSocket(): ResourceSocket
    {
        $worker                     = $this->getSelfWorker();

        if ($worker === null) {
            throw new \Error('Wrong usage of the method getServerSocketFactory(). This method can be used only inside the worker!');
        }

        try {
            $socket                 = Ipc\connect($this->uri, $this->key, new TimeoutCancellation($this->ipcTimeout));

            if ($socket instanceof ResourceSocket) {
                return $socket;
            }

            throw new \RuntimeException('Type of socket is not ResourceSocket');

        } catch (\Throwable $exception) {
            throw new \RuntimeException('Could not connect to IPC socket', 0, $exception);
        }
    }

    private function handleMessage(mixed $message, int $workerId = 0): void
    {
        if ($this->isSelfWorker()) {
            $this->workerHandler($message);
        } elseif ($this->getWorkerPool() !== null) {
            $this->watcherHandler($message);
        }
    }

    private function workerHandler(mixed $message): void
    {
        if (false === $message instanceof SocketTransferInfo) {
            return;
        }

        if ($this->workerEventHandler !== null) {
            $this->getSelfWorker()?->getWorkerEventEmitter()->removeWorkerEventListener($this->workerEventHandler);
            $this->workerEventHandler = null;
        }

        if ($this->deferredFuture === null || $this->deferredFuture->isComplete()) {
            return;
        }

        $this->uri              = $message->uri;
        $this->key              = $message->key;

        $this->socketPipeFactory = new ServerSocketPipeFactory($this->createIpcForTransferSocket());
        $this->deferredFuture->complete();
    }

    private function watcherHandler(mixed $message): void
    {
        $workerPool             = $this->getWorkerPool();

        if ($workerPool === null) {
            return;
        }

        if (false === $message instanceof InitiateSocketTransfer) {
            return;
        }

        if ($message->groupId !== $this->getWorkerGroup()?->getWorkerGroupId()) {
            return;
        }

        $workerContext              = $workerPool->findWorkerContext($message->workerId);

        if ($workerContext === null) {
            return;
        }

        $workerCancellation         = $workerPool->findWorkerCancellation($message->workerId);

        try {

            $ipcHub                 = $workerPool->getIpcHub();
            $ipcKey                 = $ipcHub->generateKey();
            $socketPipeProvider     = new SocketProvider($message->workerId, $ipcHub, $ipcKey, $workerCancellation, $this->ipcTimeout);

            $workerContext->send(new SocketTransferInfo($ipcKey, $ipcHub->getUri()));

            if (\array_key_exists($message->workerId, $this->workerSocketProviders)) {
                $this->workerSocketProviders[$message->workerId]->stop();
            }

            $this->workerSocketProviders[$message->workerId] = $socketPipeProvider;

            $socketPipeProvider->start();

        } catch (\Throwable $exception) {
            if (\array_key_exists($message->workerId, $this->workerSocketProviders)) {
                $this->workerSocketProviders[$message->workerId]->stop();
                unset($this->workerSocketProviders[$message->workerId]);
            }

            $workerPool->getLogger()?->error('Could not send socket transfer info to worker', ['exception' => $exception]);
        }
    }

    public function __serialize(): array
    {
        return ['ipcTimeout' => $this->ipcTimeout];
    }

    public function __unserialize(array $data): void
    {
        $this->ipcTimeout           = $data['ipcTimeout'] ?? 5;
    }
}
