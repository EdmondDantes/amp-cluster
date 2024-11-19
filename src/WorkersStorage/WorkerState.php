<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\WorkersStorage;

class WorkerState implements WorkerStateInterface
{
    private bool $reviewOnly        = false;

    public static function instanciateFromStorage(
        WorkersStorageInterface|null $storage,
        int $workerId,
        bool $reviewOnly            = false
    ): WorkerStateInterface {
        $workerState                = new static($workerId);
        $workerState->storage       = false === $reviewOnly ? \WeakReference::create($storage) : null;
        $workerState->reviewOnly    = $reviewOnly;

        return $workerState;
    }

    private \WeakReference|null $storage    = null;

    public function __construct(
        public readonly int  $workerId      = 0,
        public int  $groupId                = 0,
        public bool $shouldBeStarted         = false,
        public int  $shutdownErrors          = 0,
        public bool $isReady                 = false,
        public int  $pid                     = 0,
        public int  $totalReloaded           = 0,
        public int  $weight                  = 0,
        public int  $firstStartedAt          = 0,
        public int  $startedAt               = 0,
        public int  $finishedAt              = 0,
        public int  $updatedAt               = 0,
        public int  $phpMemoryUsage          = 0,
        public int  $phpMemoryPeakUsage      = 0,
        public int  $connectionsAccepted     = 0,
        public int  $connectionsProcessed    = 0,
        public int  $connectionsErrors       = 0,
        public int  $connectionsRejected     = 0,
        public int  $connectionsProcessing   = 0,
        public int  $jobAccepted             = 0,
        public int  $jobProcessed            = 0,
        public int  $jobProcessing           = 0,
        public int  $jobErrors               = 0,
        public int  $jobRejected             = 0
    ) {
    }

    public function getWorkerId(): int
    {
        return $this->workerId;
    }

    public function getGroupId(): int
    {
        return $this->groupId;
    }

    public function setGroupId(int $groupId): static
    {
        $this->groupId = $groupId;
        return $this;
    }

    public function isShouldBeStarted(): bool
    {
        return $this->shouldBeStarted;
    }

    public function markAsShouldBeStarted(): static
    {
        $this->shouldBeStarted = true;
        return $this;
    }

    public function isReady(): bool
    {
        return $this->isReady;
    }

    public function markAsReady(): static
    {
        $this->isReady = true;
        return $this;
    }

    public function markAsUnReady(): static
    {
        $this->isReady = false;
        return $this;
    }

    public function markAsShutdown(): static
    {
        $this->isReady              = false;
        $this->finishedAt           = \time();
        $this->pid                  = 0;
        return $this;
    }

    public function getPid(): int
    {
        return $this->pid;
    }

    public function setPid(int $pid): static
    {
        $this->pid = $pid;
        return $this;
    }

    public function getTotalReloaded(): int
    {
        return $this->totalReloaded;
    }

    public function setTotalReloaded(int $totalReloaded): static
    {
        $this->totalReloaded = $totalReloaded;
        return $this;
    }

    public function incrementTotalReloaded(): static
    {
        $this->totalReloaded++;
        return $this;
    }

    public function getShutdownErrors(): int
    {
        return $this->shutdownErrors;
    }

    public function setShutdownErrors(int $shutdownErrors): static
    {
        $this->shutdownErrors = $shutdownErrors;
        return $this;
    }

    public function incrementShutdownErrors(): static
    {
        $this->shutdownErrors++;
        return $this;
    }

    public function getWeight(): int
    {
        return $this->weight;
    }

    public function setWeight(int $weight): static
    {
        $this->weight = $weight;
        return $this;
    }

    public function increaseWeight(int $weight): static
    {
        $this->weight += $weight;
        return $this;
    }

    public function decreaseWeight(int $weight): static
    {
        $this->weight -= $weight;
        return $this;
    }

    public function getFirstStartedAt(): int
    {
        return $this->firstStartedAt;
    }

    public function setFirstStartedAt(int $firstStartedAt): static
    {
        $this->firstStartedAt = $firstStartedAt;
        return $this;
    }

    public function getStartedAt(): int
    {
        return $this->startedAt;
    }

    public function setStartedAt(int $startedAt): static
    {
        $this->startedAt = $startedAt;
        return $this;
    }

    public function getFinishedAt(): int
    {
        return $this->finishedAt;
    }

    public function setFinishedAt(int $finishedAt): static
    {
        $this->finishedAt = $finishedAt;
        return $this;
    }

    public function getUpdatedAt(): int
    {
        return $this->updatedAt;
    }

    public function setUpdatedAt(int $updatedAt): static
    {
        $this->updatedAt = $updatedAt;
        return $this;
    }

    public function getPhpMemoryUsage(): int
    {
        return $this->phpMemoryUsage;
    }

    public function setPhpMemoryUsage(int $phpMemoryUsage): static
    {
        $this->phpMemoryUsage = $phpMemoryUsage;
        return $this;
    }

    public function getPhpMemoryPeakUsage(): int
    {
        return $this->phpMemoryPeakUsage;
    }

    public function setPhpMemoryPeakUsage(int $phpMemoryPeakUsage): static
    {
        $this->phpMemoryPeakUsage = $phpMemoryPeakUsage;
        return $this;
    }

    public function getConnectionsAccepted(): int
    {
        return $this->connectionsAccepted;
    }

    public function setConnectionsAccepted(int $connectionsAccepted): static
    {
        $this->connectionsAccepted = $connectionsAccepted;
        return $this;
    }

    public function incrementConnectionsAccepted(): static
    {
        $this->connectionsAccepted++;
        return $this;
    }

    public function getConnectionsProcessed(): int
    {
        return $this->connectionsProcessed;
    }

    public function setConnectionsProcessed(int $connectionsProcessed): static
    {
        $this->connectionsProcessed = $connectionsProcessed;
        return $this;
    }

    public function incrementConnectionsProcessed(): static
    {
        $this->connectionsProcessed++;
        return $this;
    }

    public function getConnectionsErrors(): int
    {
        return $this->connectionsErrors;
    }

    public function setConnectionsErrors(int $connectionsErrors): static
    {
        $this->connectionsErrors = $connectionsErrors;
        return $this;
    }

    public function incrementConnectionsErrors(): static
    {
        $this->connectionsErrors++;
        return $this;
    }

    public function getConnectionsRejected(): int
    {
        return $this->connectionsRejected;
    }

    public function setConnectionsRejected(int $connectionsRejected): static
    {
        $this->connectionsRejected = $connectionsRejected;
        return $this;
    }

    public function incrementConnectionsRejected(): static
    {
        $this->connectionsRejected++;
        return $this;
    }

    public function getConnectionsProcessing(): int
    {
        return $this->connectionsProcessing;
    }

    public function setConnectionsProcessing(int $connectionsProcessing): static
    {
        $this->connectionsProcessing = $connectionsProcessing;
        return $this;
    }

    public function incrementConnectionsProcessing(): static
    {
        $this->connectionsProcessing++;
        return $this;
    }

    public function decrementConnectionsProcessing(): static
    {
        $this->connectionsProcessing--;
        return $this;
    }

    public function getJobAccepted(): int
    {
        return $this->jobAccepted;
    }

    public function setJobAccepted(int $jobAccepted): static
    {
        $this->jobAccepted = $jobAccepted;
        return $this;
    }

    public function incrementJobAccepted(): static
    {
        $this->jobAccepted++;
        return $this;
    }

    public function getJobProcessed(): int
    {
        return $this->jobProcessed;
    }

    public function setJobProcessed(int $jobProcessed): static
    {
        $this->jobProcessed = $jobProcessed;
        return $this;
    }

    public function incrementJobProcessed(): static
    {
        $this->jobProcessed++;
        return $this;
    }

    public function getJobProcessing(): int
    {
        return $this->jobProcessing;
    }

    public function setJobProcessing(int $jobProcessing): static
    {
        $this->jobProcessing = $jobProcessing;
        return $this;
    }

    public function incrementJobProcessing(): static
    {
        $this->jobProcessing++;
        return $this;
    }

    public function decrementJobProcessing(): static
    {
        $this->jobProcessing--;
        return $this;
    }

    public function getJobErrors(): int
    {
        return $this->jobErrors;
    }

    public function setJobErrors(int $jobErrors): static
    {
        $this->jobErrors = $jobErrors;
        return $this;
    }

    public function incrementJobErrors(): static
    {
        $this->jobErrors++;
        return $this;
    }

    public function getJobRejected(): int
    {
        return $this->jobRejected;
    }

    public function setJobRejected(int $jobRejected): static
    {
        $this->jobRejected = $jobRejected;
        return $this;
    }

    public function incrementJobRejected(): static
    {
        $this->jobRejected++;
        return $this;
    }

    public function jobEnqueued(int $weight, bool $canAcceptMoreJobs): void
    {
        $this->jobAccepted++;
        $this->jobProcessing++;

        $this->weight               += $weight;
        $this->isReady              = $canAcceptMoreJobs;

        $this->updateJobSegment()->updateStateSegment();
    }

    public function jobDequeued(int $weight, bool $canAcceptMoreJobs): void
    {
        $this->jobProcessed++;
        $this->jobProcessing--;

        $this->weight               -= $weight;
        $this->isReady              = $canAcceptMoreJobs;

        $this->updateJobSegment()->updateStateSegment();
    }

    public function initDefaults(): static
    {
        $this->read();

        $now                        = \time();

        if ($this->firstStartedAt === 0) {
            $this->firstStartedAt   = $now;
        }

        $this->shouldBeStarted      = true;
        $this->weight               = 0;
        $this->startedAt            = $now;
        $this->updatedAt            = $now;
        $this->totalReloaded++;

        $this->phpMemoryUsage       = \memory_get_usage(true);
        $this->phpMemoryPeakUsage   = \memory_get_peak_usage(true);

        return $this;
    }

    public function markUsShutdown(): static
    {
        $this->read();

        $this->shouldBeStarted      = false;
        $this->isReady              = false;
        $this->startedAt            = 0;
        $this->finishedAt           = \time();
        $this->pid                  = 0;
        $this->weight               = 0;
        $this->updatedAt            = \time();

        $this->phpMemoryUsage       = 0;
        $this->phpMemoryPeakUsage   = 0;
        $this->connectionsProcessing= 0;
        $this->jobProcessing        = 0;

        $this->update();
        return $this;
    }

    protected function getStorage(): WorkersStorageInterface|null
    {
        return $this->storage?->get();
    }

    public static function getItemSize(): int
    {
        // 1. Each integer value is 8 bytes.
        // 2. Each boolean value is 8 bytes.
        return 32 * 8;
    }

    public function read(): static
    {
        $data                       = $this->getStorage()?->readWorkerState($this->workerId);

        if ($data === null) {
            return $this;
        }

        $data                       = \unpack('Q*', $data);

        if (false === $data) {
            throw new \RuntimeException('Failed to read worker state');
        }

        // Ignore empty data
        if (\count($data) === 0) {
            return $this;
        }

        $data[3]                    = (bool) $data[3];
        $data[5]                    = (bool) $data[5];

        [,
            $this->groupId,

            $this->shouldBeStarted,
            $this->shutdownErrors,

            $this->isReady,
            $this->pid,
            $this->totalReloaded,
            $this->weight,

            $this->firstStartedAt,
            $this->startedAt,
            $this->finishedAt,
            $this->updatedAt,

            $this->phpMemoryUsage,
            $this->phpMemoryPeakUsage,

            $this->connectionsAccepted,
            $this->connectionsProcessed,
            $this->connectionsErrors,
            $this->connectionsRejected,
            $this->connectionsProcessing,

            $this->jobAccepted,
            $this->jobProcessed,
            $this->jobProcessing,
            $this->jobErrors,
            $this->jobRejected
        ] = \array_values($data);

        return $this;
    }

    public function update(): static
    {
        $this->checkReviewOnly();
        $this->getStorage()?->updateWorkerState($this->workerId, $this->packItem());

        return $this;
    }

    public function updateStateSegment(): static
    {
        $this->checkReviewOnly();

        [$data, $offset]            = $this->packStateSegment();

        $this->getStorage()?->updateWorkerState($this->workerId, $data, $offset);

        return $this;
    }

    public function updateTimeSegment(): static
    {
        $this->checkReviewOnly();

        [$data, $offset]            = $this->packTimeSegment();

        $this->getStorage()?->updateWorkerState($this->workerId, $data, $offset);

        return $this;
    }

    public function updateMemorySegment(): static
    {
        $this->checkReviewOnly();

        [$data, $offset]            = $this->packMemorySegment();

        $this->getStorage()?->updateWorkerState($this->workerId, $data, $offset);

        return $this;
    }

    public function updateConnectionsSegment(): static
    {
        $this->checkReviewOnly();

        [$data, $offset]            = $this->packConnectionSegment();

        $this->getStorage()?->updateWorkerState($this->workerId, $data, $offset);

        return $this;
    }

    public function updateJobSegment(): static
    {
        $this->checkReviewOnly();

        [$data, $offset]            = $this->packJobSegment();

        $this->getStorage()?->updateWorkerState($this->workerId, $data, $offset);

        return $this;
    }

    public function updateShouldBeStarted(bool $shouldBeStarted): static
    {
        $this->shouldBeStarted       = $shouldBeStarted;

        $this->getStorage()?->updateWorkerState($this->workerId, \pack('Q', (int) $shouldBeStarted), 2 * 8);
        return $this;
    }

    public function increaseAndUpdateShutdownErrors(int $count = 1): static
    {
        $this->shutdownErrors       += $count;

        $this->getStorage()?->updateWorkerState($this->workerId, \pack('Q', $this->shutdownErrors), 3 * 8);
        return $this;
    }

    protected function packItem(): string
    {
        return \pack(
            'Q*',
            // offset 0 * 8
            $this->workerId,
            $this->groupId,

            // offset 2 * 8
            $this->shouldBeStarted,
            $this->shutdownErrors,

            // offset 4 * 8
            $this->isReady,
            $this->pid,
            $this->totalReloaded,
            $this->weight,

            // offset 8 * 8
            $this->firstStartedAt,
            $this->startedAt,
            $this->finishedAt,
            $this->updatedAt,

            // offset 12 * 8
            $this->phpMemoryUsage,
            $this->phpMemoryPeakUsage,

            // offset 14 * 8
            $this->connectionsAccepted,
            $this->connectionsProcessed,
            $this->connectionsErrors,
            $this->connectionsRejected,
            $this->connectionsProcessing,

            // offset 19 * 8
            $this->jobAccepted,
            $this->jobProcessed,
            $this->jobProcessing,
            $this->jobErrors,
            $this->jobRejected
        );
    }

    public static function unpackItem(string $packedItem, int $workerId = 0): WorkerStateInterface
    {
        $unpackedItem               = \unpack('Q*', $packedItem);

        if (false === $unpackedItem) {
            throw new \RuntimeException('Failed to unpack worker state');
        }

        if (empty($unpackedItem[1])) {
            $unpackedItem[1]        = $workerId;
        }

        return new WorkerState(
            $unpackedItem[1] ?? $workerId,
            $unpackedItem[2] ?? 0,
            (bool) ($unpackedItem[3] ?? false),
            $unpackedItem[4] ?? 0,
            (bool) ($unpackedItem[5] ?? false),
            $unpackedItem[6] ?? 0,
            $unpackedItem[7] ?? 0,
            $unpackedItem[8] ?? 0,
            $unpackedItem[9] ?? 0,
            $unpackedItem[10] ?? 0,
            $unpackedItem[11] ?? 0,
            $unpackedItem[12] ?? 0,
            $unpackedItem[13] ?? 0,
            $unpackedItem[14] ?? 0,
            $unpackedItem[15] ?? 0,
            $unpackedItem[16] ?? 0,
            $unpackedItem[17] ?? 0,
            $unpackedItem[18] ?? 0,
            $unpackedItem[19] ?? 0,
            $unpackedItem[20] ?? 0,
            $unpackedItem[21] ?? 0,
            $unpackedItem[22] ?? 0,
            $unpackedItem[23] ?? 0,
            $unpackedItem[24] ?? 0
        );
    }

    protected function packStateSegment(): array
    {
        return [
            \pack(
                'Q*',
                $this->isReady,
                $this->pid,
                $this->totalReloaded,
                $this->weight
            ),
            4 * 8
            ];
    }

    protected function packTimeSegment(): array
    {
        return [
            \pack(
                'Q*',
                $this->firstStartedAt,
                $this->startedAt,
                $this->finishedAt,
                $this->updatedAt
            ),
            8 * 8
        ];
    }

    protected function packMemorySegment(): array
    {
        return [
            \pack(
                'Q*',
                $this->phpMemoryUsage,
                $this->phpMemoryPeakUsage
            ),
            12 * 8
        ];
    }

    protected function packConnectionSegment(): array
    {
        return [
            \pack(
                'Q*',
                $this->connectionsAccepted,
                $this->connectionsProcessed,
                $this->connectionsErrors,
                $this->connectionsRejected,
                $this->connectionsProcessing
            ),
            14 * 8
        ];
    }

    protected function packJobSegment(): array
    {
        return [
            \pack(
                'Q*',
                $this->jobAccepted,
                $this->jobProcessed,
                $this->jobProcessing,
                $this->jobErrors,
                $this->jobRejected
            ),
            19 * 8
        ];
    }

    protected function checkReviewOnly(): void
    {
        if ($this->reviewOnly) {
            throw new \RuntimeException('Worker state review only is enabled');
        }
    }
}
