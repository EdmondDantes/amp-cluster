<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\SocketStrategy\Windows;

use Amp\Socket\SocketAddress;
use IfCastle\AmpPool\EventWeakHandler;
use IfCastle\AmpPool\Strategies\SocketStrategy\Windows\Messages\MessageSocketListen;
use IfCastle\AmpPool\WatcherEvents\WorkerProcessTerminating;
use IfCastle\AmpPool\WorkerGroupInterface;
use IfCastle\AmpPool\WorkerPool;

final class SocketListenerProvider
{
    /** @var array<string, SocketClientListenerProvider> */
    private array $socketListeners  = [];
    private mixed $eventListener;

    public function __construct(private readonly WorkerPool $workerPool, private readonly WorkerGroupInterface $workerGroup)
    {
        $self                   = \WeakReference::create($this);

        $this->eventListener    = new EventWeakHandler(
            $this,
            static function (mixed $event, int $workerId = 0) use ($self) {
                $self->get()?->eventListener($event, $workerId);
            }
        );

        $this->workerPool->getWorkerEventEmitter()->addWorkerEventListener($this->eventListener);
    }

    private function listen(int $workerId, SocketAddress|string $address): void
    {
        if (false === $address instanceof SocketAddress) {
            // Normalize to SocketAddress here to avoid throwing exception for invalid strings at a receiving end.
            $address                = SocketAddress\fromString($address);
        }

        $stringAddress              = (string) $address;

        if (\array_key_exists($stringAddress, $this->socketListeners)) {
            $this->socketListeners[$stringAddress]->addWorker($workerId);
            return;
        }

        $this->socketListeners[$stringAddress] = new SocketClientListenerProvider($address, $this->workerPool, $this->workerGroup);
        $this->socketListeners[$stringAddress]->addWorker($workerId);
        $this->socketListeners[$stringAddress]->startListen();
    }

    public function close(): void
    {
        $this->socketListeners = [];

        if ($this->eventListener !== null) {
            $this->workerPool->getWorkerEventEmitter()->removeWorkerEventListener($this->eventListener);
            $this->eventListener = null;
        }
    }

    private function eventListener(mixed $event, int $workerId = 0): void
    {
        if ($event instanceof MessageSocketListen) {
            $this->listen($workerId, $event->address);
        }

        if ($event instanceof WorkerProcessTerminating) {
            $this->removeWorker($workerId);
            //
            // Hack!
            // For windows, we have some issues when we try to create a Server socket again after closing it.
            //
            // $this->cleanListeners();
        }
    }

    private function removeWorker(int $workerId): void
    {
        foreach ($this->socketListeners as $listener) {
            $listener->removeWorker($workerId);
        }
    }

    private function cleanListeners(): void
    {
        foreach ($this->socketListeners as $address => $listener) {
            if ($listener->stopIfNoWorkers()) {
                unset($this->socketListeners[$address]);
            }
        }
    }
}
