<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\JobIpc;

use Amp\ByteStream\StreamChannel;
use Amp\ByteStream\StreamException;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Future;
use Amp\Serialization\PassthroughSerializer;
use Amp\Sync\ChannelException;
use Amp\TimeoutCancellation;
use Amp\TimeoutException;
use IfCastle\AmpPool\Exceptions\NoWorkersAvailable;
use IfCastle\AmpPool\Exceptions\SendJobException;
use IfCastle\AmpPool\WorkerGroupInterface;
use Revolt\EventLoop;
use function Amp\delay;
use function Amp\Socket\socketConnector;

/**
 * The class is responsible for sending JOBs to other workers.
 */
final class IpcClient implements IpcClientInterface
{
    use ForbidCloning;
    use ForbidSerialization;

    /**
     * @var StreamChannel[]
     */
    private array                       $workerChannels = [];
    private JobSerializerInterface|null $jobSerializer  = null;
    /**
     * List of futures that are waiting for the result of the job with SocketId, and time when the job was sent.
     * @var array [Future, int, int]
     */
    private array $resultsFutures   = [];
    private int $maxTryCount        = 3;
    private int $futureTimeout      = 60 * 10;
    private string $futureTimeoutCallbackId;

    /**
     * IpcClient constructor.
     *
     * @param JobSerializerInterface|null $jobSerializer Job serializer
     * @param Cancellation|null           $cancellation  Cancellation
     * @param int                         $retryInterval Retry interval for sending a job
     */
    public function __construct(
        private readonly int $workerId,
        private readonly WorkerGroupInterface $workerGroup,
        private readonly array $groupsScheme,
        ?JobSerializerInterface                $jobSerializer = null,
        private readonly Cancellation|null    $cancellation = null,
        private readonly int                  $retryInterval = 1,
        private readonly int                  $scalingTimeout = 2
    ) {
        if ($this->workerGroup->getPickupStrategy() === null) {
            throw new \InvalidArgumentException('WorkerGroup must have a PickupStrategy');
        }

        $this->jobSerializer        = $jobSerializer ?? new JobSerializer();
    }

    public function mainLoop(): void
    {
        $this->futureTimeoutCallbackId = EventLoop::repeat($this->futureTimeout / 2, $this->updateFuturesByTimeout(...));
    }

    /**
     * @inheritDoc
     */
    public function sendJob(string $data, array $allowedGroups = [], array $allowedWorkers = [], bool $awaitResult = false, int $priority = 0, int $weight = 0): Future|null
    {
        $deferred                   = null;

        if ($awaitResult) {
            $deferred               = new DeferredFuture();
        }

        EventLoop::queue(function () use ($data, $allowedGroups, $allowedWorkers, $deferred, $priority, $weight) {

            //
            // We always catch and suppress exceptions at this point because otherwise,
            // they could disrupt the entire process.
            //

            try {
                $this->sendJobImmediately($data, $allowedGroups, $allowedWorkers, $deferred ?? false, $priority, $weight);
            } catch (\Throwable $exception) {
                if ($deferred instanceof DeferredFuture && false === $deferred->isComplete()) {
                    $deferred->complete($exception);
                }
            }
        });

        return $deferred?->getFuture();
    }

    /**
     * Try to send a job to the worker immediately in the current fiber.
     *
     *
     * @throws \Throwable
     */
    public function sendJobImmediately(
        string              $data,
        array               $allowedGroups       = [],
        array               $allowedWorkers      = [],
        bool|DeferredFuture $awaitResult         = false,
        int                 $priority            = 0,
        int                 $weight              = 0
    ): Future|null {
        $tryCount                   = 0;
        $ignoreWorkers              = [];

        if ($awaitResult instanceof DeferredFuture) {
            $deferred               = $awaitResult;
        } else {
            $deferred               = $awaitResult ? new DeferredFuture() : null;
        }

        if ($allowedGroups === []) {
            $allowedGroups          = $this->workerGroup->getJobGroups();
        }

        // Add self-worker to ignore-list
        if (false === \in_array($this->workerId, $ignoreWorkers, true)) {
            $ignoreWorkers[]        = $this->workerId;
        }

        while ($tryCount < $this->maxTryCount) {

            $tryCount++;
            $isScalingPossible      = false;
            $foundedWorkerId        = $this->pickupWorker($allowedGroups, $allowedWorkers, $ignoreWorkers, $priority, $weight, $tryCount);

            try {

                if ($foundedWorkerId === null) {
                    $isScalingPossible  = $this->requestScaling($allowedGroups);
                    throw new NoWorkersAvailable($allowedGroups);
                }

                $socketId           = $this->tryToSendJob($foundedWorkerId, $data, $priority, $weight, $deferred);

                if ($deferred !== null) {
                    $this->resultsFutures[\spl_object_id($deferred)] = [$deferred, $socketId, \time()];
                    return $deferred->getFuture();
                }
                return null;

            } catch (NoWorkersAvailable $exception) {

                if ($tryCount >= $this->maxTryCount) {
                    $deferred?->complete($exception);
                    throw $exception;
                }

                if ($isScalingPossible && $this->scalingTimeout > 0) {
                    // suspend the current task for a while
                    delay($this->scalingTimeout, true, $this->cancellation);
                } elseif ($this->retryInterval > 0) {
                    // suspend the current task for a while
                    delay((float) $this->retryInterval, true, $this->cancellation);
                } else {
                    $deferred?->complete($exception);
                    throw $exception;
                }

            } catch (StreamException) {
                $ignoreWorkers[]    = $foundedWorkerId;
            }
        }

        if ($deferred !== null) {
            $deferred->complete(new SendJobException($allowedGroups, $this->maxTryCount));
            return $deferred->getFuture();
        }

        throw new SendJobException($allowedGroups, $this->maxTryCount);
    }

    private function tryToSendJob(
        $foundedWorkerId,
        string $data,
        int $priority               = 0,
        int $weight                 = 0,
        ?DeferredFuture $deferred    = null
    ): int {
        $channel                    = $this->getWorkerChannel($foundedWorkerId);
        $jobId                      = $deferred !== null ? \spl_object_id($deferred) : 0;

        try {
            $channel->send(
                $this->jobSerializer->createRequest($jobId, $this->workerId, $this->workerGroup->getWorkerGroupId(), $data, $priority, $weight)
            );
        } catch (\Throwable $exception) {
            $deferred->complete($exception);
            throw $exception;
        }

        return \spl_object_id($channel);
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        EventLoop::cancel($this->futureTimeoutCallbackId);

        $channels                   = $this->workerChannels;
        $this->workerChannels       = [];

        foreach ($channels as $channel) {
            try {
                // Close connection gracefully
                $channel->send(IpcServer::CLOSE_HAND_SHAKE);
                $channel->close();
            } catch (\Throwable) {
            }
        }
    }

    private function pickupWorker(
        array $allowedGroups        = [],
        array $allowedWorkers       = [],
        array $ignoreWorkers        = [],
        int   $priority             = 0,
        int   $weight               = 0,
        int   $tryCount             = 0
    ): int|null {
        if ($allowedGroups === []) {
            $allowedGroups          = $this->workerGroup->getJobGroups();
        }

        // Add self-worker to ignore a list
        if (false === \in_array($this->workerId, $ignoreWorkers, true)) {
            $ignoreWorkers[]        = $this->workerId;
        }

        return $this->workerGroup->getPickupStrategy()?->pickupWorker(
            $allowedGroups,
            $allowedWorkers,
            $ignoreWorkers,
            $priority,
            $weight,
            $tryCount
        );
    }

    private function requestScaling(array $allowedGroups): bool
    {
        $workerId                   = $this->workerId;
        $groupsScheme               = $this->groupsScheme;
        $isPossible                 = false;

        foreach ($allowedGroups as $groupId) {

            if (\array_key_exists($groupId, $groupsScheme) === false) {
                continue;
            }

            if ($groupsScheme[$groupId]->getScalingStrategy()?->requestScaling($workerId) === true) {
                $isPossible         = true;
            }
        }

        return $isPossible;
    }

    private function getWorkerChannel(int $workerId): StreamChannel
    {
        if (\array_key_exists($workerId, $this->workerChannels)) {
            return $this->workerChannels[$workerId];
        }

        $this->workerChannels[$workerId] = $this->createWorkerChannel($workerId);

        EventLoop::queue($this->readLoop(...), $workerId);

        return $this->workerChannels[$workerId];
    }

    private function createWorkerChannel(int $workerId): StreamChannel
    {
        $connector                  = socketConnector();

        $client                     = $connector->connect(
            IpcServer::getSocketAddress($workerId),
            cancellation: new TimeoutCancellation(5)
        );

        $client->write(IpcServer::HAND_SHAKE);

        return new StreamChannel($client, $client, new PassthroughSerializer);
    }

    private function readLoop(int $workerId): void
    {
        $channel                    = $this->workerChannels[$workerId] ?? null;

        if ($channel === null) {
            return;
        }

        try {
            while (($data = $channel->receive($this->cancellation)) !== null) {

                $response           = $this->jobSerializer->parseResponse($data);

                if (\array_key_exists($response->getJobId(), $this->resultsFutures)) {
                    [$deferred, ] = $this->resultsFutures[$response->getJobId()];
                    unset($this->resultsFutures[$response->getJobId()]);
                    $deferred->complete($response->getData());
                }
            }
        } catch (\Throwable $exception) {

            unset($this->workerChannels[$workerId]);

            try {
                $channel->send(IpcServer::CLOSE_HAND_SHAKE);
                $channel->close();
            } catch (\Throwable) {
            }

            // Ignore the exception if it is not a ChannelException
            if (false === $exception instanceof ChannelException
               && false === $exception instanceof TimeoutException
               && false === $exception instanceof CancelledException) {
                throw $exception;
            }
        }
    }

    private function updateFuturesByTimeout(): void
    {
        $currentTime                = \time();

        foreach ($this->resultsFutures as $id => [$deferred, $socketId, $time]) {
            if ($currentTime - $time > $this->futureTimeout) {
                unset($this->resultsFutures[$id]);
                $deferred->error(new TimeoutException('Future timeout: ' . $this->futureTimeout));
            }
        }
    }
}
