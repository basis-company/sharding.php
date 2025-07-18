<?php

namespace Basis\Sharding\Trait;

trait ActiveRecord
{
    use ShardingDatabase;

    public function delete(): self
    {
        return $this->getShardingDatabase()->delete($this);
    }

    public function update(array|string $key, $value = null): self
    {
        $changes = is_array($key) ? $key : [$key => $value];
        return $this->getShardingDatabase()->update($this, $changes);
    }
}
