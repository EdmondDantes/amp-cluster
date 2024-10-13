<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\RunnerStrategy;

use Amp\Future;
use Amp\Parallel\Context\Context;
use Amp\Serialization\SerializationException;
use Amp\Sync\Channel;
use Amp\Sync\ChannelException;
use Amp\TimeoutCancellation;
use IfCastle\AmpPool\Exceptions\FatalWorkerException;
use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;
use IfCastle\AmpPool\Worker\Worker;
use IfCastle\AmpPool\Worker\WorkerEntryPointInterface;
use IfCastle\AmpPool\WorkerGroupInterface;
use function Amp\async;

class DefaultRunner extends WorkerStrategyAbstract implements RunnerStrategyInterface
{
    /**
     * It's the entry point for the worker process.
     *
     *
     *
     * @throws \Throwable
     */
    public static function processEntryPoint(Channel $channel): void
    {
        if (\function_exists('posix_setsid')) {
            // Allow accepting signals (like SIGINT), without having signals delivered to the watcher impact the cluster
            posix_setsid();
        }

        try {
            ['id' => $id, 'group' => $group, 'groupsScheme' => $groupsScheme, 'workersStorage' => $workersStorage, 'context' => $context]
                = self::readWorkerMetadata($channel);

        } catch (\Throwable $exception) {
            throw new FatalWorkerException('Could not connect to IPC socket', 0, $exception);
        }

        self::setProcessTitle($group->getGroupName().' worker #'.$id. ' group #'.$group->getWorkerGroupId());

        $worker                     = null;

        try {

            $entryPointClassName    = $group->getEntryPointClass();

            if (\class_exists($entryPointClassName)) {
                $entryPoint         = new $entryPointClassName();
            } else {
                throw new FatalWorkerException('Entry point class not found: ' . $entryPointClassName);
            }

            if (false === $entryPoint instanceof WorkerEntryPointInterface) {
                throw new FatalWorkerException('Entry point class must implement WorkerEntryPointI');
            }

            $worker                 = new Worker((int) $id, $channel, $group, $groupsScheme, $workersStorage, null, $context);

            $entryPoint->initialize($worker);
            $worker->applyGlobalErrorHandler();
            $worker->initWorker();

            $referenceWorker        = \WeakReference::create($worker);
            $referenceEntryPoint    = \WeakReference::create($entryPoint);

            /** @psalm-suppress InvalidArgument */
            Future\await([
                async(static function () use ($referenceWorker): void {
                    $referenceWorker->get()?->mainLoop();
                }),
                async(static function () use ($referenceEntryPoint, $referenceWorker): void {
                    try {
                        $referenceEntryPoint->get()?->run();
                    } catch (\Throwable $throwable) {
                        $referenceWorker->get()?->stop($throwable);
                    } finally {
                        $referenceWorker->get()?->stop();
                    }
                }),
            ]);

            // Normally, the worker process will exit when the IPC channel is closed.
            $worker->awaitShutdown();

        } catch (\Throwable $throwable) {
            $worker?->stop($throwable);
        } finally {
            $worker?->stop();
        }
    }

    public function getScript(): string|array
    {
        return __DIR__ . '/runner.php';
    }

    /**
     * @throws SerializationException
     * @throws ChannelException
     */
    public function initiateWorkerContext(Context $processContext, int $workerId, WorkerGroupInterface $group, array $context = []): void
    {
        $workerPool                 = $this->getWorkerPool();

        if($workerPool === null) {
            throw new \RuntimeException('Worker pool is not defined.');
        }

        $processContext->send([
            'id'                    => $workerId,
            'group'                 => $group,
            'groupsScheme'          => $workerPool->getGroupsScheme(),
            'workersStorage'        => $workerPool->getWorkersStorage()::class,
            'context'               => $context
        ]);
    }

    protected static function readWorkerMetadata(Channel $channel): array
    {
        // Read random IPC hub URI and associated key from a process channel.
        $data                   = $channel->receive(
            new TimeoutCancellation(2, 'Worker <== Watcher: The waiting time for the message has expired.')
        );

        if(empty($data)) {
            throw new FatalWorkerException('Worker <== Watcher: Could not read IPC data from channel');
        }

        foreach (['id', 'group', 'groupsScheme', 'workersStorage'] as $key) {
            if (false === \array_key_exists($key, $data)) {
                throw new FatalWorkerException('Worker <== Watcher: Invalid IPC data received. Expected key: '.$key);
            }
        }

        if(false === $data['group'] instanceof WorkerGroupInterface) {
            throw new \Error('Invalid group type. Expected WorkerGroupInterface');
        }

        if(!\is_array($data['groupsScheme'])) {
            throw new \Error('Invalid groups scheme. Expected array');
        }
        
        if(false === array_key_exists('context', $data)) {
            $data['context']        = [];
        }

        return $data;
    }

    public static function setProcessTitle(string $title): void
    {
        if (\function_exists('cli_set_process_title')) {

            \set_error_handler(static fn () => true);

            try {
                \cli_set_process_title($title);
            } finally {
                \restore_error_handler();
            }
        }
    }
}
