<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\RestartStrategy;

use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;

/**
 * The worker will not be restarted if the following conditions are met:
 *
 * The previous restart occurred within the time window specified in the parameter $windowDuration.
 * The number of restart attempts within the time window has been exhausted.
 * An error occurred if the parameter $isError is specified.
 */
final class RestartWithWindowLimiter extends WorkerStrategyAbstract implements RestartStrategyInterface
{
    private int $restartsCount      = 0;
    private ?int $currentInterval   = null;
    private int $lastRestartWindow  = 0;

    public function __construct(
        private readonly bool $isError = true,
        private readonly int $maxRestarts = 3,
        private readonly int $windowDuration = 10,
        private readonly int $restartInterval = 0,
        private readonly int $step = 10,
        private readonly int $intervalThreshold = 120,
    ) {
    }

    public function shouldRestart(mixed $exitResult): int
    {
        if ($this->isError && false === $exitResult instanceof \Throwable) {
            return RestartStrategyInterface::RESTART_IMMEDIATELY;
        }

        if (($this->lastRestartWindow + $this->windowDuration) <= \time()) {
            $this->currentInterval  = null;
            $this->restartsCount    = 0;
        }

        if ($this->restartsCount >= $this->maxRestarts) {
            return RestartStrategyInterface::RESTART_NEVER;
        }

        $this->restartsCount++;

        if ($this->lastRestartWindow === 0) {
            $this->lastRestartWindow = \time();
        }

        if ($this->currentInterval === null) {
            $this->currentInterval = $this->restartInterval;
            return $this->currentInterval;
        }

        $this->currentInterval      += $this->step;

        if ($this->currentInterval < $this->intervalThreshold) {
            return $this->currentInterval;
        }

        return $this->intervalThreshold;
    }

    public function getFailReason(): string
    {
        return 'Interval: ' . $this->currentInterval . 's, restarts: ' . $this->restartsCount . ' of ' . $this->maxRestarts . ' allowed';
    }
}
