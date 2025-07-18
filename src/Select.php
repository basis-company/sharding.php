<?php

declare(strict_types=1);

namespace Basis\Sharding;

use ArrayIterator;
use Closure;
use Iterator;

class Select implements Iterator
{
    /**
     * @var Where[]
     */
    public array $conditions = [];
    public bool $orderByAscending = true;
    public int $limit = 0;
    public string $orderBy = '';

    public ?ArrayIterator $iterator = null;

    public function __construct(
        public readonly Closure $callback,
    ) {
    }

    public function asc(string $field): self
    {
        return $this->orderBy($field, true);
    }

    public function desc(string $field): self
    {
        return $this->orderBy($field, false);
    }

    public function limit(int $limit): self
    {
        $this->limit = $limit;
        return $this;
    }

    public function where(string $field): Where
    {
        $parent = array_key_exists($field, $this->conditions) ? $this->conditions[$field] : null;
        return $this->conditions[$field] = new Where($this, $parent);
    }

    public function orderBy(string $field, bool $ascending = true): self
    {
        $this->orderBy = $field;
        $this->orderByAscending = $ascending;

        return $this;
    }

    public function getIterator(): ArrayIterator
    {
        if ($this->iterator === null) {
            $this->iterator = new ArrayIterator(call_user_func($this->callback, $this));
        }

        return $this->iterator;
    }

    public function current(): mixed
    {
        return $this->getIterator()->current();
    }

    public function key(): mixed
    {
        return $this->getIterator()->key();
    }

    public function next(): void
    {
        $this->getIterator()->next();
    }
    public function rewind(): void
    {
        $this->getIterator()->rewind();
    }

    public function valid(): bool
    {
        return $this->getIterator()->valid();
    }

    public function toArray(): array
    {
        return iterator_to_array($this->getIterator());
    }
}
