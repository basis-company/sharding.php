<?php

declare(strict_types=1);

namespace Basis\Sharding;

class Where
{
    public ?int $isGreaterThan = null;

    public function __construct(
        public readonly Select $select,
        public readonly ?Where $parent = null,
    ) {
    }

    public function isGreaterThan(int $value): Select
    {
        $this->isGreaterThan = $value;
        return $this->select;
    }

    /**
     * @return Where[]
     */
    public function getConditions(): array
    {
        return array_merge([$this], $this->parent ? $this->parent->getConditions() : []);
    }
}
