<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\JobIpc;

use IfCastle\AmpPool\Exceptions\RemoteException;

final class JobSerializer implements JobSerializerInterface
{
    final const int REQUEST_HEADER_LENGTH = 5 * 4;
    final const int REQUEST_HEADER_ITEMS = 5;
    final const int RESPONSE_HEADER_LENGTH = 4 * 4;
    final const int RESPONSE_HEADER_ITEMS = 4;

    public function createRequest(int $jobId, int $fromWorkerId, int $workerGroupId, string $data, int $priority = 0, int $weight = 0): string
    {
        return \pack('V*', $jobId, $fromWorkerId, $workerGroupId, $priority, $weight) . $data;
    }

    public function parseRequest(string $request): JobRequestInterface
    {
        // Check minimum length
        if (\strlen($request) < self::REQUEST_HEADER_LENGTH) {
            throw new \InvalidArgumentException('Request is too short (less '.self::REQUEST_HEADER_LENGTH . ' bytes)');
        }

        $buffer                     = \unpack('V*', \substr($request, 0, self::REQUEST_HEADER_LENGTH));

        if (false === $buffer || \count($buffer) !== self::REQUEST_HEADER_ITEMS) {
            throw new \RuntimeException('Failed to unpack data for request');
        }

        [, $jobId, $fromWorkerId, $workerGroupId, $priority, $weight] = $buffer;
        $data                       = \substr($request, self::REQUEST_HEADER_LENGTH);

        return new JobRequest($jobId, $fromWorkerId, $workerGroupId, $priority, $weight, $data);
    }

    public function createResponse(int $jobId, int $fromWorkerId, int $workerGroupId, string|\Throwable $data): string
    {
        $isException                = $data instanceof \Throwable ? 1 : 0;

        if ($data instanceof \Throwable) {

            if (false === $data instanceof RemoteException) {
                $data               = new RemoteException($data->getMessage(), 0, $data);
            }

            $data                   = \serialize($data);
        }

        return \pack('V*', $jobId, $fromWorkerId, $workerGroupId, $isException).$data;
    }

    public function parseResponse(string $response): JobResponseInterface
    {
        // Check minimum length
        if (\strlen($response) < self::RESPONSE_HEADER_LENGTH) {
            throw new \InvalidArgumentException('Response is too short (less '.self::RESPONSE_HEADER_LENGTH . ' bytes)');
        }

        $buffer                     = \unpack('V*', \substr($response, 0, self::RESPONSE_HEADER_LENGTH));

        if (false === $buffer || \count($buffer) !== self::RESPONSE_HEADER_ITEMS) {
            throw new \RuntimeException('Failed to unpack data for response');
        }

        [, $jobId, $fromWorkerId, $workerGroupId, $isError] = $buffer;

        $data                       = \substr($response, self::RESPONSE_HEADER_LENGTH);
        $exception                  = null;

        if ($isError) {
            $exception              = \unserialize($data);
            $data                   = '';

            if (false === $exception) {
                $exception          = new RemoteException('Failed to unserialize response data');
            }
        }

        return new JobResponse($jobId, $fromWorkerId, $workerGroupId, $isError, $data, $exception);
    }
}
