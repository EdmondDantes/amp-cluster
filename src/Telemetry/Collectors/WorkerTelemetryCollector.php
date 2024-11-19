<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Telemetry\Collectors;

use IfCastle\AmpPool\WorkersStorage\WorkerStateInterface;
use Psr\Log\LoggerInterface;

class WorkerTelemetryCollector implements ConnectionCollectorInterface, JobCollectorInterface
{
    private int $firstErrorAt       = 0;
    private int $errorsCount        = 0;

    public function __construct(
        private readonly WorkerStateInterface $workerState,
        private readonly ?LoggerInterface $logger = null,
        private readonly int $errorsTimeout = 60 * 60
    ) {
    }

    public function flushTelemetry(): void
    {
        try {

            // Update memory usage
            $this->workerState->setPhpMemoryUsage(\memory_get_usage(true));
            $this->workerState->setPhpMemoryPeakUsage(\memory_get_peak_usage(true));
            $this->workerState->updateMemorySegment();

            $this->workerState->updateConnectionsSegment()->updateJobSegment();
        } catch (\Throwable $exception) {
            $this->errorsCount++;

            if ($this->firstErrorAt === 0) {
                $this->firstErrorAt = \time();
            }

            if ($this->errorsCount <= 1 || \time() - $this->firstErrorAt > $this->errorsTimeout) {
                $this->logger?->error(
                    'Telemetry error: '.$exception->getMessage(),
                    ['file' => $exception->getFile(), 'line' => $exception->getLine(), 'trace' => $exception->getTraceAsString()]
                );

                $this->firstErrorAt = 0;
                $this->errorsCount  = 0;
            }
        }
    }

    public function connectionAccepted(): void
    {
        $this->workerState->incrementConnectionsAccepted();
    }

    public function connectionProcessing(): void
    {
        $this->workerState->incrementConnectionsProcessing();
    }

    public function connectionUnProcessing(bool $withError = false): void
    {
        $this->workerState->decrementConnectionsProcessing();
        $this->workerState->incrementConnectionsProcessed();

        if ($withError) {
            $this->workerState->incrementConnectionsErrors();
        }
    }

    public function connectionProcessed(): void
    {
        $this->workerState->incrementConnectionsProcessed();
    }

    public function connectionError(): void
    {
        $this->workerState->incrementConnectionsErrors();
    }

    public function jobAccepted(): void
    {
        $this->workerState->incrementJobAccepted();
    }

    public function jobProcessed(): void
    {
        $this->workerState->incrementJobProcessed();
    }

    public function jobError(): void
    {
        $this->workerState->incrementJobErrors();
    }

    public function jobRejected(): void
    {
        $this->workerState->incrementJobRejected();
    }

    public function jobProcessing(): void
    {
        $this->workerState->incrementJobProcessing();
    }

    public function jobUnProcessing(bool $withError = false): void
    {
        $this->workerState->decrementJobProcessing();
        $this->workerState->incrementJobProcessed();

        if ($withError) {
            $this->workerState->incrementJobErrors();
        }
    }
}
