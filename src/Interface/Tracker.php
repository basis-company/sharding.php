<?php

namespace Basis\Sharding\Interface;

interface Tracker
{
    public function setContext(array $context): void;
    public function track(string $table, string $listener): void;

    public function ackChanges(string $listener, array $changes): void;
    public function getChanges(string $listener, int $limit = 100): array;
}
