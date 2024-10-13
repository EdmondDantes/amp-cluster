<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\RunnerStrategy;

use Amp\Parallel\Context\Context;
use IfCastle\AmpPool\WorkerGroupInterface;

interface RunnerStrategyInterface
{
    public function getScript(): string|array;

    /**
     * Send initial context from the `Watcher` process to a `Worker` process.
     * Returns a key to identify the connection between the `Watcher` and the `Worker`.
     *
     *
     **/
    public function initiateWorkerContext(Context $processContext, int $workerId, WorkerGroupInterface $group, array $context = []): void;
}
