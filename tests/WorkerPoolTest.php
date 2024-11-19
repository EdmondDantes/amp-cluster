<?php
declare(strict_types=1);

namespace IfCastle\AmpPool;

use Amp\Sync\ChannelException;
use IfCastle\AmpPool\Exceptions\FatalWorkerException;
use IfCastle\AmpPool\Strategies\RestartStrategy\RestartNever;
use IfCastle\AmpPool\WorkerPoolMocks\EntryPointHello;
use IfCastle\AmpPool\WorkerPoolMocks\EntryPointWait;
use IfCastle\AmpPool\WorkerPoolMocks\FatalWorkerEntryPoint;
use IfCastle\AmpPool\WorkerPoolMocks\RestartEntryPoint;
use IfCastle\AmpPool\WorkerPoolMocks\RestartStrategies\RestartNeverWithLastError;
use IfCastle\AmpPool\WorkerPoolMocks\RestartStrategies\RestartTwice;
use IfCastle\AmpPool\WorkerPoolMocks\Runners\RunnerLostChannel;
use IfCastle\AmpPool\WorkerPoolMocks\StartCounterEntryPoint;
use IfCastle\AmpPool\WorkerPoolMocks\TerminateWorkerEntryPoint;
use IfCastle\AmpPool\WorkerPoolMocks\TestEntryPointWaitTermination;
use IfCastle\AmpPool\WorkersStorage\WorkersStorage;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use function Amp\delay;

class WorkerPoolTest extends TestCase
{
    #[RunInSeparateProcess]
    public function testStart(): void
    {
        EntryPointHello::removeFile();

        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            EntryPointHello::class,
            WorkerTypeEnum::SERVICE,
            minWorkers:                2,
            restartStrategy:           new RestartNever
        ));

        $workerPool->run();

        $this->assertFileExists(EntryPointHello::getFile());

        EntryPointHello::removeFile();
    }

    #[RunInSeparateProcess]
    public function testStop(): void
    {
        TestEntryPointWaitTermination::removeFile();

        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            TestEntryPointWaitTermination::class,
            WorkerTypeEnum::SERVICE,
            minWorkers: 2,
            restartStrategy: new RestartNever
        ));

        EventLoop::delay(0.2, fn () => $workerPool->stop());

        $workerPool->run();

        $this->assertFileExists(TestEntryPointWaitTermination::getFile());
    }

    #[RunInSeparateProcess]
    public function testAwaitStart(): void
    {
        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            TestEntryPointWaitTermination::class,
            WorkerTypeEnum::SERVICE,
            minWorkers: 2,
            restartStrategy: new RestartNever
        ));

        $awaitDone                  = false;

        EventLoop::queue(function () use ($workerPool, &$awaitDone) {

            $workerPool->awaitStart();
            $awaitDone              = true;
            $workerPool->stop();
        });

        $workerPool->run();

        $this->assertTrue($awaitDone, 'Await start should be done');
    }

    #[RunInSeparateProcess]
    public function testRestart(): void
    {
        $restartStrategy            = new RestartTwice;
        RestartEntryPoint::removeFile();

        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            RestartEntryPoint::class,
            WorkerTypeEnum::SERVICE,
            minWorkers: 1,
            restartStrategy: $restartStrategy
        ));

        EventLoop::delay(0.2, fn () => $workerPool->restart());

        $workerPool->run();

        $this->assertEquals(0, $restartStrategy->restarts);
        $this->assertFileExists(RestartEntryPoint::getFile());
        $this->assertEquals(1, (int) \file_get_contents(RestartEntryPoint::getFile()));
    }

    #[RunInSeparateProcess]
    public function testStartWithMinZero(): void
    {
        EntryPointHello::removeFile();

        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            EntryPointHello::class,
            WorkerTypeEnum::SERVICE,
            maxWorkers:                1,
            restartStrategy:           new RestartNever
        ));

        $workerPool->run();

        $this->assertFileDoesNotExist(EntryPointHello::getFile());

        EntryPointHello::removeFile();
    }

    #[RunInSeparateProcess]
    public function testStartWithMaxZero(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Max workers must be a positive integer');

        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            EntryPointHello::class,
            WorkerTypeEnum::SERVICE,
            maxWorkers:                0,
            restartStrategy:           new RestartNever
        ));

        $workerPool->run();
    }

    #[RunInSeparateProcess]
    public function testFatalWorkerException(): void
    {
        $restartStrategy            = new RestartTwice;

        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            FatalWorkerEntryPoint::class,
            WorkerTypeEnum::SERVICE,
            minWorkers: 1,
            restartStrategy: $restartStrategy
        ));

        $exception                  = null;

        try {
            $workerPool->run();
        } catch (\Throwable $exception) {
        }

        $this->assertInstanceOf(FatalWorkerException::class, $exception);
        $this->assertEquals(0, $restartStrategy->restarts, 'Worker should not be restarted');
    }

    #[RunInSeparateProcess]
    public function testTerminateWorkerException(): void
    {
        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            TerminateWorkerEntryPoint::class,
            WorkerTypeEnum::SERVICE,
            minWorkers:      2,
            restartStrategy: new RestartNever
        ));

        $workerPool->run();
        $this->assertTrue(true, 'Workers should be terminated without any exception');
    }

    /**
     * Check if pool state is updated after worker started.
     *
     * @throws \Throwable
     */
    #[RunInSeparateProcess]
    public function testWorkerState(): void
    {
        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            EntryPointWait::class,
            WorkerTypeEnum::SERVICE,
            minWorkers:      2,
            restartStrategy: new RestartNever
        ));

        $workerPool->describeGroup(new WorkerGroup(
            EntryPointWait::class,
            WorkerTypeEnum::SERVICE,
            minWorkers:      3,
            restartStrategy: new RestartNever
        ));

        $workers                     = null;
        $expectedWorkers             = [1 => true, 2 => true, 3 => true, 4 => true, 5 => true];

        EventLoop::queue(function () use ($workerPool, &$workers) {

            $workerPool->awaitStart();

            foreach (WorkersStorage::instanciate()->foreachWorkers() as $workerState) {
                $workers[$workerState->getWorkerId()] = $workerState->isReady();
            }

            $workerPool->stop();
        });

        $workerPool->run();

        $this->assertEquals($expectedWorkers, $workers, 'The First group have worker id 1 and 2, the second group have worker id 3, 4 and 5');

        // Check pool state after workers stopped

        foreach (WorkersStorage::instanciate() as $workerState) {
            $this->assertFalse($workerState->isReady(), 'All workers should be unready');
        }
    }

    #[RunInSeparateProcess]
    public function testChannelLost(): void
    {
        $restartStrategy            = new RestartNeverWithLastError;

        $workerPool                 = new WorkerPool;

        $workerPool->describeGroup(new WorkerGroup(
            EntryPointHello::class,
            WorkerTypeEnum::SERVICE,
            minWorkers     :           1,
            runnerStrategy :           new RunnerLostChannel,
            restartStrategy:           $restartStrategy
        ));

        $exception                  = null;

        try {
            $workerPool->run();
        } catch (\Throwable $exception) {
        }

        if ($exception !== null) {
            $this->assertInstanceOf(FatalWorkerException::class, $exception);
        }

        if ($exception === null) {
            $this->assertInstanceOf(ChannelException::class, $restartStrategy->lastError);
        }
    }

    #[RunInSeparateProcess]
    public function testScale(): void
    {
        StartCounterEntryPoint::removeFile();

        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            StartCounterEntryPoint::class,
            WorkerTypeEnum::SERVICE,
            minWorkers:                1,
            maxWorkers:                5,
            restartStrategy:           new RestartNever
        ));

        EventLoop::delay(1, function () use ($workerPool) {
            // Scale workers to 3 (1 + 2)
            $workerPool->scaleWorkers(1, 2);
            $workerPool->awaitStart();
            $workerPool->stop();
        });

        $workerPool->run();

        $this->assertFileExists(StartCounterEntryPoint::getFile());
        $this->assertEquals(3, (int) \file_get_contents(StartCounterEntryPoint::getFile()));

        StartCounterEntryPoint::removeFile();
    }

    #[RunInSeparateProcess]
    public function testSoftRestart(): void
    {
        $workerPool                 = new WorkerPool;
        $workerPool->describeGroup(new WorkerGroup(
            EntryPointHello::class,
            WorkerTypeEnum::SERVICE,
            minWorkers:                1
        ));

        EventLoop::delay(1, function () use ($workerPool) {

            $state                  = $workerPool->getWorkersStorage()->getApplicationState();
            $pid                    = $state->getPid();

            $this->assertEquals(0, $state->getRestartsCount(), 'Restarts count should be 0');
            $this->assertEquals(\getmypid(), $state->getPid(), 'Pid should be the same as the main process');

            $workerPool->restart(true);
            delay(2);

            $workerStorageReadOnly  = WorkersStorage::instanciate();
            $state                  = $workerStorageReadOnly->getApplicationState();
            $state->read();

            $this->assertEquals(1, $state->getRestartsCount(), 'Restarts count should be 1');
            $this->assertEquals($pid, $state->getPid(), 'Pid should be the same as the main process');

            $workerPool->stop();
        });

        $workerPool->run();

        $this->assertTrue(true, 'Soft restart should not throw any exception');
    }
}
