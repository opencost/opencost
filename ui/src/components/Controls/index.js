import React from 'react'
import { makeStyles } from '@material-ui/styles'
import EditControl from './Edit'
import DownloadControl from './Download'

const useStyles = makeStyles({
  controls: {
    flexGrow: 0,
    minWidth: 200,
  },
})

const Controls = ({
  windowOptions,
  window,
  setWindow,
  aggregationOptions,
  aggregateBy,
  setAggregateBy,
  accumulateOptions,
  accumulate,
  setAccumulate,
  idleOptions,
  idle,
  setIdle,
  title,
  cumulativeData,
  currency,
  titleField,
  filters,
  setFilters,
  clearContext,
  context,
}) => {
  const classes = useStyles()

  let reportAggregateBy = aggregateBy
  if (context.length > 0) {
    reportAggregateBy = context[0].property.toLowerCase()
  }

  return (
    <div className={classes.controls}>
      <EditControl
        windowOptions={windowOptions}
        window={window}
        setWindow={setWindow}
        aggregationOptions={aggregationOptions}
        aggregateBy={aggregateBy}
        setAggregateBy={setAggregateBy}
        accumulateOptions={accumulateOptions}
        accumulate={accumulate}
        setAccumulate={setAccumulate}
        idleOptions={idleOptions}
        idle={idle}
        setIdle={setIdle}
        filters={filters}
        setFilters={setFilters}
        currency={currency}
        clearContext={clearContext}
      />

      <DownloadControl
        cumulativeData={cumulativeData}
        title={title}
      />
    </div>
  )
}

export default React.memo(Controls)
