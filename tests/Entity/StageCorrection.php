<?php

namespace Basis\Sharding\Test\Entity;

use Tarantool\Mapper\Entity;

class StageCorrection extends Entity
{
    /**
     * @var integer
     */
    public $id;

    /**
     * @var Stage
     * @required
     */
    public $stage;

    /**
    * @var integer
    * @required
    */
    public $begin;

    /**
    * @var integer
    * @required
    */
    public $end;

    /**
    * @var integer
    * @required
    */
    public $person;

    /**
    * @var integer
    * @required
    */
    public $timestamp;

    /**
     * @var integer
     * @type number
     * @required
     */
    public $delta;

    /**
     * @var integer
     * @required
     */
    public $reason;

    /**
     * @var string
     * @required
     */
    public $data;

    public function calculateDelta()
    {
        $this->delta = 0;

        $volume = $this->app->dispatch('volume.render', [
            'volume' => $this->getStage()->volume,
        ]);

        $this->delta = 0;
        foreach ($volume as $shelf => $timeline) {
            if (!is_object($timeline)) {
                continue;
            }
            foreach ($timeline->summary as $summary) {
                if ($summary->space !== 'company.activity') {
                    continue;
                }
                if ($summary->instance->nick !== 'work') {
                    continue;
                }
                switch ($shelf) {
                    case 'correction':
                        $this->delta += $summary->interval->minutes;
                        break;
                    case 'telemetry':
                        $this->delta -= $summary->interval->minutes;
                        break;
                }
            }
        }
        $this->save();
    }
}