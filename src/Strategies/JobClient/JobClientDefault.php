<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\JobClient;

use Amp\DeferredFuture;
use Amp\Future;
use IfCastle\AmpPool\Exceptions\FatalWorkerException;
use IfCastle\AmpPool\JobIpc\IpcClient;
use IfCastle\AmpPool\JobIpc\IpcClientInterface;
use IfCastle\AmpPool\JobIpc\JobClientInterface;
use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;

final class JobClientDefault extends WorkerStrategyAbstract implements JobClientInterface
{
    private IpcClientInterface|null $ipcClient = null;

    public function sendJob(
        string $data,
        array  $allowedGroups       = [],
        array  $allowedWorkers      = [],
        bool   $awaitResult         = false,
        int    $priority            = 0,
        int    $weight              = 0
    ): Future|null {
        return $this->ipcClient?->sendJob($data, $allowedGroups, $allowedWorkers, $awaitResult, $priority, $weight);
    }

    public function sendJobImmediately(
        string $data,
        array $allowedGroups        = [],
        array $allowedWorkers       = [],
        bool|DeferredFuture $awaitResult = false,
        int $priority               = 0,
        int $weight                 = 0
    ): Future|null {
        return $this->ipcClient?->sendJobImmediately($data, $allowedGroups, $allowedWorkers, $awaitResult, $priority, $weight);
    }

    public function onStarted(): void
    {
        if ($this->getWorkerGroup() !== null && $this->getWorkerGroup()->getJobGroups() === []) {
            throw new FatalWorkerException(
                'JobClient strategy requires at least one JobGroup to be defined in the WorkerGroup. '
                .'Please define jobGroups before starting the WorkerPool.'
            );
        }

        $worker                     = $this->getSelfWorker();

        if ($worker === null) {
            return;
        }

        $this->ipcClient            = new IpcClient(
            $worker->getWorkerId(),
            $worker->getWorkerGroup(),
            $worker->getGroupsScheme()
        );

        $this->ipcClient->mainLoop();
    }

    public function onStopped(): void
    {
        $worker                     = $this->getWorker();

        if ($worker === null) {
            return;
        }

        $this->ipcClient?->close();
        $this->ipcClient            = null;
    }
}
