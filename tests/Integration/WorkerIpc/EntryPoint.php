<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Integration\WorkerIpc;

use Amp\Cancellation;
use Amp\TimeoutCancellation;
use IfCastle\AmpPool\Coroutine\CoroutineInterface;
use IfCastle\AmpPool\Exceptions\FatalWorkerException;
use IfCastle\AmpPool\Strategies\JobExecutor\JobHandlerInterface;
use IfCastle\AmpPool\Worker\WorkerEntryPointInterface;
use IfCastle\AmpPool\Worker\WorkerInterface;
use IfCastle\AmpPool\WorkerTypeEnum;
use Revolt\EventLoop;

final class EntryPoint implements WorkerEntryPointInterface, JobHandlerInterface
{
    public const string GROUP1      = 'group1';
    public const string GROUP2      = 'group2';
    public const string WAS_HANDLED = ' was handled';

    public static function getFile(): string
    {
        return \sys_get_temp_dir() . '/worker-integration-test.text';
    }

    public static function removeFile(): void
    {
        $file                       = self::getFile();

        if (\file_exists($file)) {
            \unlink($file);
        }

        if (\file_exists($file)) {
            throw new \RuntimeException('Could not remove file: ' . $file);
        }
    }

    private WorkerInterface $worker;

    public function initialize(WorkerInterface $worker): void
    {
        $this->worker               = $worker;

        if ($worker->getWorkerGroup()->getWorkerType() === WorkerTypeEnum::JOB) {
            $worker->getWorkerGroup()->getJobExecutor()->defineJobHandler($this);
        }
    }

    public function handleJob(
        string             $data,
        ?CoroutineInterface $coroutine = null,
        ?Cancellation       $cancellation = null
    ): mixed {
        EventLoop::delay(1, fn () => $this->worker->stop());

        return $data.self::WAS_HANDLED;
    }

    public function run(): void
    {
        $group                      = $this->worker->getWorkerGroup();

        if ($group->getWorkerType() === WorkerTypeEnum::REACTOR) {
            $future                 = $group->getJobClient()?->sendJobImmediately($group->getGroupName(), awaitResult: true);

            if ($future === null) {
                throw new FatalWorkerException('Could not send job to client');
            }

            $result                 = $future->await(new TimeoutCancellation(5));

            \file_put_contents(self::getFile(), $result);
        } else {
            $this->worker->awaitTermination();
        }
    }
}
