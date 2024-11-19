<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\RestartStrategy;

use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;

/**
 * Restart worker with interval limiter.
 *
 * The worker will be restarted with an increasing interval between restarts.
 */
final class RestartWithLimiter extends WorkerStrategyAbstract implements RestartStrategyInterface
{
    private int $restartsCount      = 0;
    private ?int $currentInterval   = null;

    public function __construct(
        private readonly int $maxRestarts = 3,
        private readonly int $restartInterval = 60,
        private readonly int $step = 10,
        private readonly int $intervalThreshold = 120,
    ) {
    }

    public function shouldRestart(mixed $exitResult): int
    {
        if ($this->restartsCount >= $this->maxRestarts) {
            return RestartStrategyInterface::RESTART_NEVER;
        }

        $this->restartsCount++;

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
