<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Telemetry\Prometheus;

use Amp\Http\HttpStatus;
use Amp\Http\Server\DefaultErrorHandler;
use Amp\Http\Server\RequestHandler\ClosureRequestHandler;
use Amp\Http\Server\Response;
use Amp\Http\Server\SocketHttpServer;
use IfCastle\AmpPool\Exceptions\FatalWorkerException;
use IfCastle\AmpPool\Worker\WorkerEntryPointInterface;
use IfCastle\AmpPool\Worker\WorkerInterface;
use Monolog\Logger;

class PrometheusService implements WorkerEntryPointInterface
{
    public const string MIME_TYPE = 'text/plain; version=0.0.4';

    protected \WeakReference $worker;

    protected function getWorker(): WorkerEntryPointInterface
    {
        return $this->worker->get();
    }

    public function initialize(WorkerInterface $worker): void
    {
        $this->worker = \WeakReference::create($worker);
    }

    public function run(): void
    {
        $worker                     = $this->worker->get();

        if ($worker === null) {
            return;
        }

        $prometheusProvider         = new PrometheusProvider($worker->getWorkersStorage(), $worker->getGroupsScheme());
        $httpServer                 = SocketHttpServer::createForDirectAccess(new Logger('prometheus'));

        $workerGroup                = $worker->getWorkerGroup();

        if ($workerGroup instanceof PrometheusGroup === false) {
            throw new FatalWorkerException('The worker group must be an instance of PrometheusGroup');
        }

        $address                    = $workerGroup->getPrometheusAddress();

        // 2. Expose the server to the network
        $httpServer->expose($address);

        $worker->getLogger()->info('Prometheus service is running on '.$address);

        // 3. Handle incoming connections and start the server
        $httpServer->start(
            new ClosureRequestHandler(static function () use ($prometheusProvider): Response {

                return new Response(
                    HttpStatus::OK,
                    ['content-type' => self::MIME_TYPE],
                    $prometheusProvider->render()
                );
            }),
            new DefaultErrorHandler(),
        );

        $worker->awaitTermination();
        $httpServer->stop();
    }
}
