<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\SocketStrategy\Windows;

use Amp\Socket\ServerSocketFactory;
use IfCastle\AmpPool\Strategies\SocketStrategy\SocketStrategyInterface;
use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;

final class SocketWindowsStrategy extends WorkerStrategyAbstract implements SocketStrategyInterface
{
    private SocketListenerProvider|null $socketListenerProvider = null;
    private SocketPipeFactoryWindows|null $socketPipeFactory = null;

    public function getServerSocketFactory(): ServerSocketFactory|null
    {
        if ($this->socketPipeFactory !== null) {
            return $this->socketPipeFactory;
        }

        $worker                     = $this->getSelfWorker();

        if ($worker === null) {
            throw new \Error('Wrong usage of the method getServerSocketFactory(). This method can be used only inside the worker!');
        }

        $this->socketPipeFactory    = new SocketPipeFactoryWindows($worker->getWatcherChannel(), $worker);

        return $this->socketPipeFactory;
    }

    public function onStarted(): void
    {
        $workerPool                 = $this->getWorkerPool();

        if ($workerPool === null) {
            return;
        }

        $this->socketListenerProvider = new SocketListenerProvider($workerPool, $this->getWorkerGroup());
    }

    public function onStopped(): void
    {
        $this->socketListenerProvider?->close();
        $this->socketListenerProvider   = null;
        $this->socketPipeFactory        = null;
    }
}
