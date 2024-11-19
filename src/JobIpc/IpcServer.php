<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\JobIpc;

use Amp\ByteStream\ReadableResourceStream;
use Amp\ByteStream\StreamChannel;
use Amp\Cancellation;
use Amp\CancelledException;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Pipeline\Queue;
use Amp\Serialization\PassthroughSerializer;
use Amp\Socket;
use Amp\Socket\SocketAddress;
use Amp\Socket\SocketException;
use Amp\Sync\Channel;
use Amp\Sync\ChannelException;
use Amp\TimeoutCancellation;
use Amp\TimeoutException;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;
use const Amp\Process\IS_WINDOWS;
use function Amp\delay;

/**
 * Allows organizing a connection pool for communication between workers.
 * The method getJobQueue() returns the task queue where Job, accepted via an IPC channel, is written.
 */
final class IpcServer implements IpcServerInterface
{
    use ForbidCloning;
    use ForbidSerialization;

    public const string HAND_SHAKE = 'AM PHP WORKER IPC';
    public const string CLOSE_HAND_SHAKE = 'AM PHP WORKER IPC CLOSE';

    private ?string $toUnlink = null;
    private Socket\ResourceServerSocket $server;
    private SocketAddress $address;

    private Queue $jobQueue;
    private JobSerializerInterface $jobSerializer;

    public static function getSocketAddress(int $workerId): SocketAddress
    {
        if (IS_WINDOWS) {
            return new Socket\InternetAddress('127.0.0.1', 10000 + $workerId);
        }
        return new Socket\UnixAddress(\sys_get_temp_dir() . '/worker-' . $workerId . '.sock');

    }

    /**
     *
     * @throws SocketException
     */
    public function __construct(
        private readonly int $workerId,
        ?JobSerializerInterface $jobSerializer       = null,
        private readonly ?LoggerInterface $logger   = null,
        private readonly int $sendResultAttempts    = 2,
        private readonly float $attemptDelay        = 0.5
    ) {
        $address                    = self::getSocketAddress($workerId);

        if (!IS_WINDOWS) {
            $this->toUnlink         = \sys_get_temp_dir() . '/worker-' . $workerId . '.sock';
        }

        // Try to remove existing file
        if ($this->toUnlink !== null && \file_exists($this->toUnlink)) {
            \unlink($this->toUnlink);
        }

        $this->address              = $address;
        $this->server               = Socket\listen($address);
        $this->jobQueue             = new Queue(10);
        $this->jobSerializer        = $jobSerializer ?? new JobSerializer();
    }

    public function __destruct()
    {
        $this->close();
    }

    public function isClosed(): bool
    {
        return $this->server->isClosed();
    }

    public function close(): void
    {
        /*
        if(false === $this->jobQueue->isComplete()) {
            $this->jobQueue->complete();
        }
        */

        $this->server->close();
        $this->unlink();
    }

    public function onClose(\Closure $onClose): void
    {
        $this->server->onClose($onClose);
    }

    public function receiveLoop(?Cancellation $cancellation = null): void
    {
        try {
            while (($client = $this->server->accept($cancellation)) !== null) {
                EventLoop::queue($this->createWorkerSocket(...), $client, $cancellation);
            }
        } catch (CancelledException) {
        }
    }

    /**
     * @return Queue<array{0: StreamChannel, 1: mixed}>
     */
    public function getJobQueue(): Queue
    {
        return $this->jobQueue;
    }

    public function sendJobResult(mixed $result, Channel $channel, JobRequest $jobRequest, ?Cancellation $cancellation = null): void
    {
        if ($result === null) {
            return;
        }

        if ($channel->isClosed()) {
            return;
        }

        try {
            $response                   = $this->jobSerializer->createResponse(
                $jobRequest->getJobId(),
                $this->workerId,
                $jobRequest->getWorkerGroupId(),
                $result
            );
        } catch (\Throwable $exception) {
            $response                   = $this->jobSerializer->createResponse(
                $jobRequest->getJobId(),
                $this->workerId,
                $jobRequest->getWorkerGroupId(),
                $exception
            );
        }

        // Try to send the result sendResultAttempts times.
        for ($i = 1; $i <= $this->sendResultAttempts; $i++) {
            try {
                $channel->send($response);
                break;
            } catch (\Throwable $exception) {
                $this->logger?->notice(
                    'Error sending job #'.$jobRequest->getJobId().' result (try number '.$i.')',
                    ['exception' => $exception, 'request' => $jobRequest]
                );

                if ($i < $this->sendResultAttempts) {
                    delay($this->attemptDelay, true, $cancellation);
                }
            }
        }
    }

    private function createWorkerSocket(
        ReadableResourceStream|Socket\Socket $stream,
        ?Cancellation $cancellation = null
    ): void {
        try {
            $this->readHandShake($stream, new TimeoutCancellation(5));
        } catch (\Throwable) {
            $stream->close();
        }

        $channel                    = new StreamChannel($stream, $stream, new PassthroughSerializer());

        EventLoop::queue(function () use ($channel, $cancellation) {

            try {
                while (($data = $channel->receive($cancellation)) !== null) {

                    if ($data === self::CLOSE_HAND_SHAKE) {
                        $channel->close();
                        break;
                    }

                    $request        = null;

                    try {
                        $request    = $this->jobSerializer->parseRequest($data);
                        $this->jobQueue->pushAsync([$channel, $request]);

                    } catch (\Throwable $exception) {

                        $channel->send($this->jobSerializer->createResponse(
                            $request?->getJobId() ?? 0,
                            $this->workerId,
                            $request?->getWorkerGroupId() ?? 0,
                            $exception
                        ));
                    }
                }
            } catch (CancelledException|TimeoutException|ChannelException) {
                // Ignore
            } finally {
                $channel->close();
            }
        });
    }

    private function readHandShake(ReadableResourceStream|Socket\Socket $stream, ?Cancellation $cancellation = null): void
    {
        $handShake                  = '';
        $length                     = \strlen(self::HAND_SHAKE);

        do {
            /** @psalm-suppress InvalidArgument */
            if (($chunk = $stream->read($cancellation, $length - \strlen($handShake))) === null) {
                throw new \RuntimeException('Failed read WorkerIpc hand shake', E_USER_ERROR);
            }

            $handShake              .= $chunk;

        } while (\strlen($handShake) < $length);

        if ($handShake !== self::HAND_SHAKE) {
            throw new \RuntimeException('Wrong WorkerIpc hand shake', E_USER_ERROR);
        }
    }

    private function unlink(): void
    {
        if ($this->toUnlink === null) {
            return;
        }

        // Ignore errors when unlinking temp socket.
        \set_error_handler(static fn () => true);
        try {
            \unlink($this->toUnlink);
        } finally {
            \restore_error_handler();
            $this->toUnlink = null;
        }
    }

    public function getAddress(): SocketAddress
    {
        return $this->address;
    }
}
