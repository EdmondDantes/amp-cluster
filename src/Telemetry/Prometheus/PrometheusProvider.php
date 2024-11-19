<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Telemetry\Prometheus;

use IfCastle\AmpPool\WorkerGroupInterface;
use IfCastle\AmpPool\WorkersStorage\WorkersStorageInterface;
use IfCastle\AmpPool\WorkersStorage\WorkerStateInterface;

final class PrometheusProvider
{
    public function __construct(
        private readonly WorkersStorageInterface $workersStorage,
        /**
         * @var WorkerGroupInterface[]
         */
        private readonly array $groupsScheme = []
    ) {
    }

    public function render(): string
    {
        $metrics                    = $this->renderApplicationState();

        $workers                    = $this->getWorkersInfo();

        $metrics                    = \array_merge($metrics, $this->renderGroupsScheme($workers));
        $metrics                    = \array_merge($metrics, $this->renderGroupsShouldBeStarted($workers));
        $metrics                    = \array_merge($metrics, $this->renderTimings($workers));
        $metrics                    = \array_merge($metrics, $this->renderUsage($workers));
        $metrics                    = \array_merge($metrics, $this->renderMemoryUsageSystem($workers));
        $metrics                    = \array_merge($metrics, $this->renderConnections($workers));
        $metrics                    = \array_merge($metrics, $this->renderJobs($workers));

        return \implode("\n", $metrics);
    }

    protected function getWorkersInfo(): array
    {
        $workers                    = $this->workersStorage->foreachWorkers();

        foreach ($workers as $worker) {
            if ($worker->getGroupId() === 0) {
                $worker->setGroupId($this->findGroupId($worker->getWorkerId()));
            }
        }

        return $workers;
    }

    protected function findGroupId(int $workerId): int
    {
        $maxWorkerId                = 1;

        foreach ($this->groupsScheme as $group) {

            $minWorkerId            = $maxWorkerId + 1;
            $maxWorkerId            = $minWorkerId + $group->getMaxWorkers();

            if ($minWorkerId <= $workerId && $workerId <= $maxWorkerId) {
                return $group->getWorkerGroupId();
            }
        }

        return 0;
    }

    protected function renderApplicationState(): array
    {
        $this->workersStorage->getApplicationState()->read();

        $metrics[] = '# TYPE application_started_at gauge';
        $metrics[] = 'application_started_at ' . $this->workersStorage->getApplicationState()->getStartedAt() * 1000;

        $metrics[] = '# TYPE application_uptime gauge';
        $metrics[] = 'application_uptime ' . (\time() - $this->workersStorage->getApplicationState()->getStartedAt());

        $metrics[] = '# TYPE application_restarts_count counter';
        $metrics[] = 'application_restarts_count ' . $this->workersStorage->getApplicationState()->getRestartsCount();

        $metrics[] = '# TYPE application_last_restarted_at gauge';
        $metrics[] = 'application_last_restarted_at ' . $this->workersStorage->getApplicationState()->getLastRestartedAt() * 1000;

        $metrics[] = '# TYPE application_workers_errors counter';
        $metrics[] = 'application_workers_errors ' . $this->workersStorage->getApplicationState()->getWorkersErrors();

        $metrics[] = '# TYPE system_load_average gauge';
        $metrics[] = 'system_load_average ' . $this->workersStorage->getApplicationState()->getLoadAverage();

        $metrics[] = '# TYPE application_memory_total gauge';
        $metrics[] = 'system_memory_total ' . $this->workersStorage->getApplicationState()->getMemoryTotal();

        $metrics[] = '# TYPE application_memory_free gauge';
        $metrics[] = 'system_memory_free ' . $this->workersStorage->getApplicationState()->getMemoryFree();

        return $metrics;
    }

    protected function renderTimings(array $workers): array
    {
        $metrics[] = '';

        $metrics[] = '# TYPE worker_first_started_at gauge';

        foreach ($workers as $workerState) {
            $metrics[] = 'worker_first_started_at{' . $this->genLabels($workerState) . '} ' . $workerState->getFirstStartedAt() * 1000;
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_start_at gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_start_at{' . $this->genLabels($workerState) . '} ' . $workerState->getStartedAt() * 1000;
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_finished_at gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_finished_at{' . $this->genLabels($workerState) . '} ' . $workerState->getFinishedAt() * 1000;
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_updated_at gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_updated_at{' . $this->genLabels($workerState) . '} ' . $workerState->getUpdatedAt() * 1000;
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_total_reloaded counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_total_reloaded{' . $this->genLabels($workerState) . '} ' . $workerState->getTotalReloaded();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_shutdown_errors counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_shutdown_errors{' . $this->genLabels($workerState) . '} ' . $workerState->getShutdownErrors();
        }

        return $metrics;
    }

    protected function renderUsage(array $workers): array
    {
        $metrics[] = '';

        $metrics[] = '# TYPE worker_weight gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_weight{' . $this->genLabels($workerState) . '} ' . $workerState->getWeight();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_php_memory_usage gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_php_memory_usage{' . $this->genLabels($workerState) . '} ' . $workerState->getPhpMemoryUsage();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_php_memory_peak_usage gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_php_memory_peak_usage{' . $this->genLabels($workerState) . '} ' . $workerState->getPhpMemoryPeakUsage();
        }

        return $metrics;
    }

    protected function renderMemoryUsageSystem(array $workers): array
    {
        $metrics[]                  = '';

        $this->workersStorage->getMemoryUsage()->read();

        $stats                      = $this->workersStorage->getMemoryUsage()->getStats();
        $index                      = 1;

        $metrics[] = '# TYPE worker_memory_usage_system gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_memory_usage_system{' . $this->genLabels($workerState) . '} ' . ($stats[$index] ?? 0);
            $index++;
        }

        return $metrics;
    }

    protected function renderConnections(array $workers): array
    {
        $metrics[] = '';

        $metrics[] = '# TYPE worker_connections_accepted counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_connections_accepted{' . $this->genLabels($workerState) . '} ' . $workerState->getConnectionsAccepted();
        }

        $metrics[] = '# TYPE worker_connections_processed counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_connections_processed{' . $this->genLabels($workerState) . '} ' . $workerState->getConnectionsProcessed();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_connections_errors counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_connections_errors{' . $this->genLabels($workerState) . '} ' . $workerState->getConnectionsErrors();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_connections_rejected counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_connections_rejected{' . $this->genLabels($workerState) . '} ' . $workerState->getConnectionsRejected();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_connections_processing gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_connections_processing{' . $this->genLabels($workerState) . '} ' . $workerState->getConnectionsProcessing();
        }

        return $metrics;
    }

    /**
     * @param WorkerStateInterface[] $workers
     *
     */
    protected function renderGroupsScheme(array $workers): array
    {
        // Calculate worker running and counts by groups
        $runningWorkers = [];

        foreach ($workers as $worker) {

            if (\array_key_exists($worker->getGroupId(), $runningWorkers) === false) {
                $runningWorkers[$worker->getGroupId()] = 0;
            }

            if ($worker->getPid() !== 0) {
                $runningWorkers[$worker->getGroupId()]++;
            }
        }

        $metrics[] = '# TYPE worker_group gauge';

        foreach ($this->groupsScheme as $group) {

            $metrics[] = 'worker_group{group_id="'.$group->getWorkerGroupId()
                         .'", group_name="' .$group->getGroupName()
                         .'", group_type="' .$group->getWorkerType()->value
                         .'", group_min_workers="' .$group->getMinWorkers()
                         .'", group_max_workers="' .$group->getMaxWorkers()
                         .'"} '.($runningWorkers[$group->getWorkerGroupId()] ?? 0);
        }

        return $metrics;
    }

    /**
     * @param WorkerStateInterface[] $workers
     *
     */
    protected function renderGroupsShouldBeStarted(array $workers): array
    {
        // Calculate worker running and counts by groups
        $shouldBeStarted = [];

        foreach ($workers as $worker) {

            if (\array_key_exists($worker->getGroupId(), $shouldBeStarted) === false) {
                $shouldBeStarted[$worker->getGroupId()] = 0;
            }

            if ($worker->isShouldBeStarted()) {
                $shouldBeStarted[$worker->getGroupId()]++;
            }
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_group_should_be_started gauge';

        foreach ($this->groupsScheme as $group) {

            $metrics[] = 'worker_group_should_be_started{group_id="'.$group->getWorkerGroupId()
                         .'", group_name="' .$group->getGroupName()
                         .'", group_type="' .$group->getWorkerType()->value
                         .'", group_min_workers="' .$group->getMinWorkers()
                         .'", group_max_workers="' .$group->getMaxWorkers()
                         .'"} '.($shouldBeStarted[$group->getWorkerGroupId()] ?? 0);
        }

        return $metrics;
    }

    protected function renderJobs(array $workers): array
    {
        $metrics[] = '';

        $metrics[] = '# TYPE worker_job_accepted counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_job_accepted{' . $this->genLabels($workerState) . '} ' . $workerState->getJobAccepted();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_job_processed counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_job_processed{' . $this->genLabels($workerState) . '} ' . $workerState->getJobProcessed();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_job_processing gauge';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_job_processing{' . $this->genLabels($workerState) . '} ' . $workerState->getJobProcessing();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_job_errors counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_job_errors{' . $this->genLabels($workerState) . '} ' . $workerState->getJobErrors();
        }

        $metrics[] = '';

        $metrics[] = '# TYPE worker_job_rejected counter';
        foreach ($workers as $workerState) {
            $metrics[] = 'worker_job_rejected{' . $this->genLabels($workerState) . '} ' . $workerState->getJobRejected();
        }

        return $metrics;
    }

    protected function genLabels(WorkerStateInterface $workerState): string
    {
        $group                      = $this->groupsScheme[$workerState->getGroupId()] ?? null;
        $groupType                  = $group?->getWorkerType()->value ?? '';

        return 'worker_id="'.$workerState->getWorkerId().'", group_id="'.$workerState->getGroupId().'", group="'.$groupType.'"';
    }
}
