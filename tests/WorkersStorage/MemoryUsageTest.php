<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\WorkersStorage;

use PHPUnit\Framework\TestCase;

class MemoryUsageTest extends TestCase
{
    public function testWriteRead(): void
    {
        $workerStorage              = WorkersStorageMemory::instanciate(5);
        $memoryUsage                = MemoryUsage::instanciate($workerStorage, 5, false);

        $this->fillMemoryUsage($memoryUsage);
        $memoryUsage->update();

        $memoryUsage2               = MemoryUsage::instanciate($workerStorage, 5, false);
        $memoryUsage2->read();

        $this->assertEquals($memoryUsage->getStats(), $memoryUsage2->getStats());
    }

    private function fillMemoryUsage(MemoryUsage $memoryUsage): void
    {
        $stats                      = [];

        for ($workerId = 1; $workerId <= 5; $workerId++) {
            $stats[$workerId]       = \rand(0, 1000);
        }

        $memoryUsage->setStats($stats);
    }
}
