import {Box, Tooltip, Colors} from '@dagster-io/ui';
import * as React from 'react';

import {useViewport} from '../gantt/useViewport';
import {RunStatus} from '../types/globalTypes';

import {assembleIntoSpans} from './SpanRepresentation';

type SelectionRange = {
  start: string;
  end: string;
};

const MIN_SPAN_WIDTH = 8;

export enum PartitionState {
  MISSING = 'missing',
  SUCCESS = 'success',
  SUCCESS_MISSING = 'success_missing', // states where the run succeeded in the past for a given step, but is missing for the last run
  FAILURE = 'failure',
  FAILURE_MISSING = 'failure_missing', // states where the run failed in the past for a given step, but is missing for the last run
  QUEUED = 'queued',
  STARTED = 'started',
}

export const runStatusToPartitionState = (runStatus: RunStatus | null) => {
  switch (runStatus) {
    case RunStatus.CANCELED:
    case RunStatus.CANCELING:
    case RunStatus.FAILURE:
      return PartitionState.FAILURE;
    case RunStatus.STARTED:
      return PartitionState.STARTED;
    case RunStatus.SUCCESS:
      return PartitionState.SUCCESS;
    case RunStatus.QUEUED:
      return PartitionState.QUEUED;
    default:
      return PartitionState.MISSING;
  }
};

export const PartitionStatus: React.FC<{
  partitionNames: string[];
  partitionStateForKey: (partitionKey: string, partitionIdx: number) => PartitionState;
  selected?: string[];
  small?: boolean;
  onClick?: (partitionName: string) => void;
  onSelect?: (selection: string[]) => void;
  splitPartitions?: boolean;
  hideStatusTooltip?: boolean;
  tooltipMessage?: string;
  selectionWindowSize?: number;
}> = ({
  partitionNames,
  partitionStateForKey,
  selected,
  onSelect,
  onClick,
  splitPartitions,
  small,
  selectionWindowSize,
  hideStatusTooltip,
  tooltipMessage,
}) => {
  const ref = React.useRef<HTMLDivElement>(null);
  const [currentSelectionRange, setCurrentSelectionRange] = React.useState<
    SelectionRange | undefined
  >();
  const {viewport, containerProps} = useViewport();

  const toPartitionName = React.useCallback(
    (e: MouseEvent) => {
      if (!ref.current) {
        return null;
      }
      const percentage =
        (e.clientX - ref.current.getBoundingClientRect().left) / ref.current.clientWidth;
      return partitionNames[Math.floor(percentage * partitionNames.length)];
    },
    [partitionNames, ref],
  );
  const getRangeSelection = React.useCallback(
    (start: string, end: string) => {
      const startIdx = partitionNames.indexOf(start);
      const endIdx = partitionNames.indexOf(end);
      return partitionNames.slice(Math.min(startIdx, endIdx), Math.max(startIdx, endIdx) + 1);
    },
    [partitionNames],
  );

  React.useEffect(() => {
    if (!currentSelectionRange || !onSelect || !selected) {
      return;
    }
    const setHoveredSelectionRange = (e: MouseEvent) => {
      const end = toPartitionName(e) || currentSelectionRange.end;
      setCurrentSelectionRange({start: currentSelectionRange?.start, end});
    };
    const setSelectionRange = (e: MouseEvent) => {
      if (!currentSelectionRange) {
        return;
      }
      const end = toPartitionName(e);
      const currentSelection = getRangeSelection(
        currentSelectionRange.start,
        end || currentSelectionRange.end,
      );
      const allSelected = currentSelection.every((name) => selected.includes(name));
      if (allSelected) {
        onSelect(selected.filter((x) => !currentSelection.includes(x)));
      } else {
        const newSelected = new Set(selected);
        currentSelection.forEach((name) => newSelected.add(name));
        onSelect(Array.from(newSelected));
      }
      setCurrentSelectionRange(undefined);
    };
    window.addEventListener('mousemove', setHoveredSelectionRange);
    window.addEventListener('mouseup', setSelectionRange);
    return () => {
      window.removeEventListener('mousemove', setHoveredSelectionRange);
      window.removeEventListener('mouseup', setSelectionRange);
    };
  }, [onSelect, selected, currentSelectionRange, getRangeSelection, toPartitionName]);

  const selectedSpans = selected
    ? assembleIntoSpans(partitionNames, (key) => selected.includes(key)).filter((s) => s.status)
    : [];
  const spans = splitPartitions
    ? partitionNames.map((name, idx) => ({
        startIdx: idx,
        endIdx: idx,
        status: partitionStateForKey(name, idx),
      }))
    : _partitionsToSpans(
        partitionNames,
        Object.fromEntries(
          partitionNames.map((name, idx) => [name, partitionStateForKey(name, idx)]),
        ),
      );

  const highestIndex = spans.map((s) => s.endIdx).reduce((prev, cur) => Math.max(prev, cur), 0);
  const indexToPct = (idx: number) => `${((idx * 100) / partitionNames.length).toFixed(3)}%`;
  const showSeparators =
    splitPartitions && viewport.width > MIN_SPAN_WIDTH * (partitionNames.length + 1);

  const _onClick = onClick
    ? (e: React.MouseEvent<any, MouseEvent>) => {
        const partitionName = toPartitionName(e.nativeEvent);
        partitionName && onClick(partitionName);
      }
    : undefined;

  const _onMouseDown = onSelect
    ? (e: React.MouseEvent<any, MouseEvent>) => {
        const name = toPartitionName(e.nativeEvent);
        if (!name) {
          return;
        }
        setCurrentSelectionRange({start: name, end: name});
      }
    : undefined;

  return (
    <div {...containerProps}>
      {selected && !selectionWindowSize ? (
        <div style={{position: 'relative', width: '100%', overflowX: 'hidden', height: 10}}>
          {selectedSpans.map((s) => (
            <div
              key={s.startIdx}
              style={{
                left: `min(calc(100% - 2px), ${indexToPct(s.startIdx)})`,
                width: indexToPct(s.endIdx - s.startIdx + 1),
                position: 'absolute',
                top: 0,
                height: 8,
                border: `2px solid ${Colors.Blue500}`,
                borderBottom: 0,
              }}
            />
          ))}
        </div>
      ) : null}
      <div
        style={{
          position: 'relative',
          width: '100%',
          height: small ? 12 : 24,
          borderRadius: 4,
          overflow: 'hidden',
          cursor: 'col-resize',
          background: Colors.Gray200,
        }}
        ref={ref}
        onClick={_onClick}
        onMouseDown={_onMouseDown}
      >
        {spans.map((s) => (
          <div
            key={s.startIdx}
            style={{
              left: `min(calc(100% - 2px), ${indexToPct(s.startIdx)})`,
              width: indexToPct(s.endIdx - s.startIdx + 1),
              minWidth: s.status && s.status !== PartitionState.MISSING ? 2 : undefined,
              position: 'absolute',
              zIndex:
                s.startIdx === 0 || s.endIdx === highestIndex
                  ? 3
                  : s.status && s.status !== PartitionState.MISSING
                  ? 2
                  : 1, //End-caps, then statuses, then missing
              top: 0,
            }}
          >
            {hideStatusTooltip || tooltipMessage ? (
              <div
                style={{
                  width: '100%',
                  height: 24,
                  outline: 'none',
                  ...partitionStateToStyle(s.status),
                }}
                title={tooltipMessage}
              />
            ) : (
              <Tooltip
                display="block"
                position="top"
                content={
                  tooltipMessage
                    ? tooltipMessage
                    : s.startIdx === s.endIdx
                    ? `Partition ${partitionNames[s.startIdx]} is ${partitionStatusToText(
                        s.status,
                      ).toLowerCase()}`
                    : `Partitions ${partitionNames[s.startIdx]} through ${
                        partitionNames[s.endIdx]
                      } are ${partitionStatusToText(s.status).toLowerCase()}`
                }
              >
                <div
                  style={{
                    width: '100%',
                    height: 24,
                    outline: 'none',
                    ...partitionStateToStyle(s.status),
                  }}
                />
              </Tooltip>
            )}
          </div>
        ))}
        {showSeparators
          ? spans.slice(1).map((s) => (
              <div
                key={`separator_${s.startIdx}`}
                style={{
                  left: `min(calc(100% - 2px), ${indexToPct(s.startIdx)})`,
                  width: 1,
                  height: small ? 14 : 24,
                  position: 'absolute',
                  zIndex: 4,
                  background: Colors.KeylineGray,
                  top: 0,
                }}
              />
            ))
          : null}
        {currentSelectionRange ? (
          <div
            key="currentSelectionRange"
            style={{
              left: `min(calc(100% - 2px), ${indexToPct(
                Math.min(
                  partitionNames.indexOf(currentSelectionRange.start),
                  partitionNames.indexOf(currentSelectionRange.end),
                ),
              )})`,
              width: indexToPct(
                Math.abs(
                  partitionNames.indexOf(currentSelectionRange.end) -
                    partitionNames.indexOf(currentSelectionRange.start),
                ) + 1,
              ),
              minWidth: 2,
              height: small ? 14 : 24,
              position: 'absolute',
              zIndex: 4,
              background: Colors.White,
              opacity: 0.7,
              top: 0,
            }}
          />
        ) : null}
        {selected && selected.length && selectionWindowSize ? (
          <>
            <div
              key="selectionRangeBackgroundLeft"
              style={{
                left: 0,
                width: indexToPct(
                  Math.min(
                    partitionNames.indexOf(selected[selected.length - 1]),
                    partitionNames.indexOf(selected[0]),
                  ),
                ),
                height: small ? 14 : 24,
                position: 'absolute',
                zIndex: 5,
                background: Colors.White,
                opacity: 0.5,
                top: 0,
              }}
            />
            <div
              key="selectionRange"
              style={{
                left: `min(calc(100% - 3px), ${indexToPct(
                  Math.min(
                    partitionNames.indexOf(selected[0]),
                    partitionNames.indexOf(selected[selected.length - 1]),
                  ),
                )})`,
                width: indexToPct(
                  Math.abs(
                    partitionNames.indexOf(selected[selected.length - 1]) -
                      partitionNames.indexOf(selected[0]),
                  ) + 1,
                ),
                minWidth: 2,
                height: small ? 14 : 24,
                position: 'absolute',
                zIndex: 5,
                border: `3px solid ${Colors.Dark}`,
                borderRadius: 4,
                top: 0,
              }}
            />
            <div
              key="selectionRangeBackgroundRight"
              style={{
                right: 0,
                width: indexToPct(
                  partitionNames.length -
                    1 -
                    Math.max(
                      partitionNames.indexOf(selected[selected.length - 1]),
                      partitionNames.indexOf(selected[0]),
                    ),
                ),
                height: small ? 14 : 24,
                position: 'absolute',
                zIndex: 5,
                background: Colors.White,
                opacity: 0.5,
                top: 0,
              }}
            />
          </>
        ) : null}
      </div>
      {!splitPartitions ? (
        <Box
          flex={{justifyContent: 'space-between'}}
          margin={{top: 4}}
          style={{fontSize: '0.8rem', color: Colors.Gray500, minHeight: 17}}
        >
          <span>{partitionNames[0]}</span>
          <span>{partitionNames[partitionNames.length - 1]}</span>
        </Box>
      ) : null}
    </div>
  );
};

function _partitionsToSpans(keys: string[], keyStatus: {[key: string]: PartitionState}) {
  const spans: {startIdx: number; endIdx: number; status: PartitionState}[] = [];

  for (let ii = 0; ii < keys.length; ii++) {
    const status: PartitionState =
      keys[ii] in keyStatus ? keyStatus[keys[ii]] : PartitionState.MISSING;
    if (!spans.length || spans[spans.length - 1].status !== status) {
      spans.push({startIdx: ii, endIdx: ii, status});
    } else {
      spans[spans.length - 1].endIdx = ii;
    }
  }

  return spans;
}

export const partitionStateToStyle = (status: PartitionState): React.CSSProperties => {
  switch (status) {
    case PartitionState.SUCCESS:
      return {background: Colors.Green500};
    case PartitionState.SUCCESS_MISSING:
      return {
        background: `linear-gradient(135deg, ${Colors.Green500} 25%, ${Colors.Gray200} 25%, ${Colors.Gray200} 50%, ${Colors.Green500} 50%, ${Colors.Green500} 75%, ${Colors.Gray200} 75%, ${Colors.Gray200} 100%)`,
        backgroundSize: '8.49px 8.49px',
      };
    case PartitionState.FAILURE:
      return {background: Colors.Red500};
    case PartitionState.STARTED:
      return {background: Colors.Blue500};
    case PartitionState.QUEUED:
      return {background: Colors.Blue200};
    default:
      return {background: Colors.Gray200};
  }
};

export const partitionStatusToText = (status: PartitionState) => {
  switch (status) {
    case PartitionState.SUCCESS:
      return 'Completed';
    case PartitionState.SUCCESS_MISSING:
      return 'Partial';
    case PartitionState.FAILURE:
      return 'Failed';
    case PartitionState.STARTED:
      return 'In progress';
    case PartitionState.QUEUED:
      return 'Queued';
    default:
      return 'Missing';
  }
};
