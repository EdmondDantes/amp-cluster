<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\SocketStrategy;

use Amp\Http\HttpStatus;
use Amp\Http\Server\DefaultErrorHandler;
use Amp\Http\Server\Driver\SocketClientFactory;
use Amp\Http\Server\RequestHandler\ClosureRequestHandler;
use Amp\Http\Server\Response;
use Amp\Http\Server\SocketHttpServer;
use IfCastle\AmpPool\Worker\WorkerEntryPointInterface;
use IfCastle\AmpPool\Worker\WorkerInterface;
use Revolt\EventLoop;

final class TestHttpReactor implements WorkerEntryPointInterface
{
    public const string ADDRESS     = '127.0.0.1:9999';

    public static function getFile(): string
    {
        return \sys_get_temp_dir() . '/worker-pool-test-window.text';
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

    private ?\WeakReference $worker = null;

    public function initialize(WorkerInterface $worker): void
    {
        $this->worker               = \WeakReference::create($worker);
    }

    public function run(): void
    {
        $worker                     = $this->worker->get();

        if ($worker instanceof WorkerInterface === false) {
            throw new \RuntimeException('The worker is not available!');
        }

        $socketFactory              = $worker->getWorkerGroup()->getSocketStrategy()?->getServerSocketFactory();

        if ($socketFactory === null) {
            throw new \RuntimeException('The socket factory is not available!');
        }

        $clientFactory              = new SocketClientFactory($worker->getLogger());
        $httpServer                 = new SocketHttpServer($worker->getLogger(), $socketFactory, $clientFactory);

        // 2. Expose the server to the network
        $httpServer->expose(self::ADDRESS);

        // 3. Handle incoming connections and start the server
        $httpServer->start(
            new ClosureRequestHandler(static function () use ($worker): Response {

                \file_put_contents(self::getFile(), __CLASS__);

                EventLoop::delay(2, static function () use ($worker) {
                    $worker->stop();
                });

                return new Response(
                    HttpStatus::OK,
                    [
                    'content-type' => 'text/plain; charset=utf-8',
                ],
                    __CLASS__
                );
            }),
            new DefaultErrorHandler(),
        );

        // 4. Await termination of the worker
        $worker->awaitTermination();

        // 5. Stop the HTTP server
        $httpServer->stop();
    }
}
