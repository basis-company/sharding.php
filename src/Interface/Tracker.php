<?php

namespace Basis\Sharding\Interface;

interface Tracker
{
    public function ackChanges(array $changes): void;
    public function getChanges(string $listener = '', int $limit = 100): array;
    public function registerChanges(string $table, string $listener): void;
    public function setContext(array $context): void;
}
