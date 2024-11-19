<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Telemetry\HttpServer;

use Amp\Http\Server\Request;
use Amp\Http\Server\Response;
use IfCastle\AmpPool\Telemetry\Collectors\ConnectionCollectorInterface;

final class RequestHandler implements \Amp\Http\Server\RequestHandler
{
    public function __construct(private readonly ConnectionCollectorInterface $collector, private readonly \Closure $closure)
    {
    }

    public function handleRequest(Request $request): Response
    {
        $this->collector->connectionAccepted();
        $this->collector->connectionProcessing();

        try {
            $response               = ($this->closure)($request);

            if ($response instanceof Response) {
                if ($response->getStatus() >= 200 && $response->getStatus() < 400) {
                    $this->collector->connectionUnProcessing();
                } else {
                    $this->collector->connectionUnProcessing(true);
                }
            } else {
                $this->collector->connectionUnProcessing(true);
            }

            return $response;

        } catch (\Throwable $exception) {
            $this->collector->connectionUnProcessing(true);
            throw $exception;
        }
    }
}
