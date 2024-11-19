<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Worker;

use Amp\Cancellation;
use Amp\Sync\Channel;
use IfCastle\AmpPool\GlobalErrorHandlerInterface;
use IfCastle\AmpPool\WorkerEventEmitterAwareInterface;
use IfCastle\AmpPool\WorkerGroup;
use IfCastle\AmpPool\WorkersStorage\WorkersStorageAwareInterface;
use IfCastle\AmpPool\WorkersStorage\WorkerStateInterface;
use IfCastle\AmpPool\WorkerTypeEnum;
use Psr\Log\LoggerInterface;

interface WorkerInterface extends WorkerEventEmitterAwareInterface, WorkersStorageAwareInterface, GlobalErrorHandlerInterface
{
    /**
     * @return array<int, WorkerGroup>
     */
    public function getGroupsScheme(): array;

    public function sendMessageToWatcher(mixed $message): void;

    public function getWatcherChannel(): Channel;

    public function getWorkerState(): WorkerStateInterface;

    public function getWorkerId(): int;
    public function getWorkerGroup(): WorkerGroup;
    public function getWorkerGroupId(): int;
    public function getWorkerType(): WorkerTypeEnum;

    /**
     * Returns the context of the worker pool.
     *
     */
    public function getPoolContext(): array;

    public function getAbortCancellation(): Cancellation;

    public function mainLoop(): void;

    public function getLogger(): LoggerInterface;

    public function awaitShutdown(): void;

    public function awaitTermination(?Cancellation $cancellation = null): void;

    public function initiateTermination(?\Throwable $throwable = null): void;

    public function stop(?\Throwable $throwable = null): void;

    public function isStopped(): bool;

    public function onClose(\Closure $onClose): void;

    public function addPeriodicTask(float $delay, \Closure $task): int;

    public function cancelPeriodicTask(int $taskId): void;

    public function defineSoftShutdownHandler(callable $handler): void;
}
