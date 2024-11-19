<?php declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\SocketStrategy\Unix;

use Amp\ByteStream\ReadableStream;
use Amp\ByteStream\ResourceStream;
use Amp\ByteStream\StreamChannel;
use Amp\ByteStream\WritableBuffer;
use Amp\Cancellation;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Serialization\NativeSerializer;
use Amp\Serialization\SerializationException;
use Amp\Serialization\Serializer;
use Amp\Socket\BindContext;
use Amp\Socket\SocketAddress;
use Amp\Socket\SocketException;
use Amp\Sync\Channel;
use Amp\Sync\ChannelException;
use const Amp\Process\IS_WINDOWS;

final class ServerSocketPipeProvider
{
    use ForbidCloning;
    use ForbidSerialization;

    private readonly BindContext $bindContext;
    private readonly Serializer $serializer;

    private static array $servers   = [];
    /**
     * Which worker is using which server socket?
     *
     * @var array<string, array<int>>
     */
    private static array $usedBy    = [];

    public function __construct(private readonly int $workerId, BindContext $bindContext = new BindContext)
    {
        $this->bindContext          = $bindContext;
        $this->serializer           = new NativeSerializer;
    }

    /**
     * @throws SocketException|SerializationException
     */
    public function provideFor(ReadableStream&ResourceStream $stream, ?Cancellation $cancellation = null): void
    {
        /** @var Channel<SocketAddress|null, never> $channel */
        $channel                    = new StreamChannel($stream, new WritableBuffer(), $this->serializer);

        /** @var StreamResourceSendPipe<SocketAddress> $pipe */
        $pipe                       = new StreamResourceSendPipe($stream, $this->serializer);

        try {
            while ($address = $channel->receive($cancellation)) {

                if (!$address instanceof SocketAddress) {
                    throw new \ValueError(
                        \sprintf(
                            'Expected only instances of %s on channel; do not use the given socket outside %s',
                            SocketAddress::class,
                            self::class,
                        )
                    );
                }

                $uri                = (string) $address;

                if (self::isUsed($uri, $this->workerId)) {
                    throw new SocketException("Socket address '$uri' already in use inside worker {$this->workerId}");
                }

                $server             = self::$servers[$uri] ??= self::bind($uri, $this->bindContext);

                self::usedBy($uri, $this->workerId);

                $pipe->send($server, $address);
            }
        } catch (ChannelException $exception) {
            throw new SocketException('Provider channel closed: ' . $exception->getMessage(), previous: $exception);
        } finally {
            self::freeWorker($this->workerId);
            $pipe->close();
        }
    }

    private static function isUsed(string $uri, int $workerId): bool
    {
        foreach (self::$usedBy as $usedUri => $workers) {
            if ($uri === $usedUri && \in_array($workerId, $workers, true)) {
                return true;
            }
        }

        return false;
    }

    private static function usedBy(string $uri, int $workerId): void
    {
        self::$usedBy[$uri][]       = $workerId;
    }

    private static function freeAddress(string $uri, int $workerId): void
    {
        if (\array_key_exists($uri, self::$usedBy) === false) {
            return;
        }

        if (($key = \array_search($workerId, self::$usedBy[$uri], true)) !== false) {
            unset(self::$usedBy[$uri][$key]);
        }
    }

    private static function freeWorker(int $workerId): void
    {
        foreach (self::$usedBy as $uri => $workers) {
            if (($key = \array_search($workerId, $workers, true)) !== false) {
                unset(self::$usedBy[$uri][$key]);
            }
        }

        self::clearAddresses();
    }

    private static function clearAddresses(): void
    {
        foreach (self::$usedBy as $uri => $workers) {

            if (empty($workers) && isset(self::$servers[$uri])) {
                \fclose(self::$servers[$uri]);
                unset(self::$servers[$uri], self::$usedBy[$uri]);

            }
        }

        foreach (self::$servers as $uri => $server) {
            if (empty(self::$usedBy[$uri])) {
                \fclose($server);
                unset(self::$servers[$uri]);
            }
        }
    }

    /**
     * @return resource
     */
    private static function bind(string $uri, BindContext $bindContext)
    {
        static $errorHandler;

        $context                    = \stream_context_create(\array_merge(
            $bindContext->toStreamContextArray(),
            [
                'socket'            => [
                  'so_reuseaddr'    => IS_WINDOWS, // SO_REUSEADDR has SO_REUSEPORT semantics on Windows
                  'ipv6_v6only'     => true,
                ],
            ],
        ));

        // Error reporting suppressed as stream_socket_server() error is immediately checked and
        // reported with an exception.
        \set_error_handler($errorHandler ??= static fn () => true);

        try {
            // Do NOT use STREAM_SERVER_LISTEN here - we explicitly invoke \socket_listen() in our worker processes
            if (!$server = \stream_socket_server($uri, $errno, $errstr, \STREAM_SERVER_BIND, $context)) {
                throw new \RuntimeException(\sprintf(
                    'Failed binding socket on %s: [Err# %s] %s',
                    $uri,
                    $errno,
                    $errstr,
                ));
            }
        } finally {
            \restore_error_handler();
        }

        return $server;
    }
}
