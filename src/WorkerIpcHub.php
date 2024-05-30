<?php
declare(strict_types=1);

namespace CT\AmpServer;

use Amp\ByteStream\ReadableResourceStream;
use Amp\ByteStream\StreamChannel;
use Amp\Cancellation;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Pipeline\Queue;
use Amp\Socket;
use Amp\Socket\SocketAddress;
use Amp\TimeoutCancellation;
use Revolt\EventLoop;
use const Amp\Process\IS_WINDOWS;

/**
 * Allows organizing a connection pool for communication between workers.
 * The method getJobQueue() returns the task queue where Job, accepted via an IPC channel, is written.
 */
final class WorkerIpcHub
{
    use ForbidCloning;
    use ForbidSerialization;
    
    public const string HAND_SHAKE = 'AM PHP WORKER IPC';
    
    private int $workerId;
    
    private ?string $toUnlink = null;
    private Socket\ResourceServerSocket $server;
    private SocketAddress $address;
    
    private Queue $jobQueue;
    
    /**
     * @param int $workerId
     *
     * @throws Socket\SocketException
     */
    public function __construct(int $workerId) {
        if (IS_WINDOWS) {
            $address                = new Socket\InternetAddress('127.0.0.1', 10000 + $workerId);
        } else {
            $path                   = \sys_get_temp_dir() . '/worker-' . $workerId . '.sock';
            $address                = new Socket\UnixAddress($path);
            $this->toUnlink         = $path;
        }
        
        $this->address              = $address;
        $this->server               = Socket\listen($address);
        $this->jobQueue             = new Queue(10);
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
        $this->jobQueue->complete();
        $this->server->close();
        $this->unlink();
    }
    
    public function onClose(\Closure $onClose): void
    {
        $this->server->onClose($onClose);
    }
    
    public function receiveLoop(Cancellation $cancellation = null): void
    {
        while (!($client = $this->server->accept($cancellation))) {
            EventLoop::queue($this->createWorkerSocket(...), $client, $cancellation);
        }
    }
    
    /**
     * @return Queue<array{0: StreamChannel, 1: mixed}>
     */
    public function getJobQueue(): Queue
    {
        return $this->jobQueue;
    }
    
    private function createWorkerSocket(
        ReadableResourceStream|Socket\Socket $stream, Cancellation $cancellation = null): void
    {
        try {
            $this->readHandShake($stream, new TimeoutCancellation(5));
        } catch (\Throwable) {
            $stream->close();
        }
        
        $channel                    = new StreamChannel($stream, $stream);
        
        EventLoop::queue(function () use ($channel, $cancellation) {
            while (($data = $channel->receive($cancellation)) !== null) {
                $this->jobQueue->pushAsync([$channel, $data]);
            }
        });
    }
    
    private function readHandShake(ReadableResourceStream|Socket\Socket $stream, ?Cancellation $cancellation = null): void
    {
        $handShake                  = '';
        $length                     = strlen(self::HAND_SHAKE);
        
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