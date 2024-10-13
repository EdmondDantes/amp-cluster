<?php
declare(strict_types=1);

namespace IfCastle\AmpPool;

use Amp\Cancellation;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Ipc\IpcHub;
use IfCastle\AmpPool\WorkersStorage\WorkersStorageAwareInterface;
use Psr\Log\LoggerInterface as PsrLogger;

interface WorkerPoolInterface extends WorkerEventEmitterAwareInterface, WorkersStorageAwareInterface, GlobalErrorHandlerInterface
{
    public function describeGroup(WorkerGroupInterface $group): self;

    public function getIpcHub(): IpcHub;

    public function getLogger(): PsrLogger|null;

    /**
     * @return WorkerGroup[]
     */
    public function getGroupsScheme(): array;

    public function findGroup(int|string $groupIdOrName): WorkerGroupInterface|null;

    public function validateGroupsScheme(): void;
    
    public function getPoolsContext(): array;

    public function setPoolContext(array $context): static;
    
    /**
     * Run the worker pool.
     * This method will block the current fiber and release it after the pool is stopped.
     *
     */
    public function run(): void;

    /**
     * The method blocks the current fiber until the worker pool is started.
     * To consider a worker properly started,
     * the system not only initiates the process
     * but also waits for the process to send a message WorkerStarted
     * confirming that it has started correctly.
     *
     */
    public function awaitStart(): void;

    /**
     * The method leads to the termination of the worker pool gracefully.
     *
     */
    public function stop(): void;

    /**
     * The method stops all workers and restarts them gracefully (not immediately).
     *
     */
    public function restart(bool $isSoft = false): void;

    public function countWorkers(int $groupId, bool $onlyRunning = false, bool $notRunning = false): int;

    public function getMainCancellation(): ?Cancellation;

    /**
     * @return int[]
     */
    public function getWorkers(): array;

    public function findWorkerContext(int $workerId): Context|null;

    public function isWorkerRunning(int $workerId): bool;

    public function findWorkerCancellation(int $workerId): Cancellation|null;

    /**
     * Scale workers in the group.
     * Returns the number of workers that were actually scaled (started or shutdown).
     *
     * The parameter $count can be negative
     * in this case, the method will try to stop the workers.
     *
     *
     */
    public function scaleWorkers(int $groupId, int $delta): int;

    /**
     * Try to restart the worker by the worker id.
     *
     * The method returns true if the worker was found and running.
     */
    public function restartWorker(int $workerId): bool;

    public function getApplicationPid(): int;

    public function getPidFile(): string;
}
