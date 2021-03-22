import Button from '@material-ui/core/Button'
import Chip from '@material-ui/core/Chip'
import FormControl from '@material-ui/core/FormControl'
import IconButton from '@material-ui/core/IconButton'
import InputLabel from '@material-ui/core/InputLabel'
import MenuItem from '@material-ui/core/MenuItem'
import Popover from '@material-ui/core/Popover'
import Select from '@material-ui/core/Select'
import TextField from '@material-ui/core/TextField'
import Tooltip from '@material-ui/core/Tooltip'
import Typography from '@material-ui/core/Typography'
import AddIcon from '@material-ui/icons/Add'
import SettingsIcon from '@material-ui/icons/Settings'
import EditIcon from '@material-ui/icons/Tune'
import { makeStyles } from '@material-ui/styles'
import { filter, isArray, sortBy, trim } from 'lodash'
import React, { useState } from 'react'
import SelectWindow from '../SelectWindow';
import { toCurrency } from '../../util';

const useStyles = makeStyles({
  chip: {
    marginRight: 4,
    marginBottom: 4,
  },
  chipIcon: {
    paddingLeft: 4,
    paddingRight: 2,
  },
  form: {
    padding: 18,
    display: 'flex',
    flexFlow: 'column',
  },
  formControl: {
    margin: 8,
    minWidth: 120,
  },
})

const EditControl = ({
  windowOptions,
  window,
  setWindow,
  aggregationOptions,
  aggregateBy,
  setAggregateBy,
  accumulateOptions,
  accumulate,
  setAccumulate,
  currency,
}) => {
  const classes = useStyles()
  const [anchorEl, setAnchorEl] = React.useState(null)

  const handleClick = (event) => {
    setAnchorEl(event.currentTarget)
  }

  const handleClose = () => {
    setAnchorEl(null)
  }

  const open = Boolean(anchorEl)
  const id = open ? 'edit-form' : undefined

  return (
    <>
      <Tooltip title="Edit report">
        <IconButton aria-describedby={id} onClick={handleClick}>
          <EditIcon />
        </IconButton>
      </Tooltip>
      <Popover
        id={id}
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'left',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'center',
        }}
      >
        <div className={classes.form}>
          <div>
            <SelectWindow
              windowOptions={windowOptions}
              window={window}
              setWindow={setWindow}
            />
            <FormControl className={classes.formControl}>
              <InputLabel id="aggregation-select-label">Breakdown</InputLabel>
              <Select
                id="aggregation-select"
                value={aggregateBy}
                onChange={e => {
                  setAggregateBy(e.target.value)
                }}
              >
                {aggregationOptions.map((opt) => <MenuItem key={opt.value} value={opt.value}>{opt.name}</MenuItem>)}
              </Select>
            </FormControl>
            <FormControl className={classes.formControl}>
              <InputLabel id="accumulate-label">Resolution</InputLabel>
              <Select
                id="accumulate"
                value={accumulate}
                onChange={e => setAccumulate(e.target.value)}
              >
                {accumulateOptions.map((opt) => <MenuItem key={opt.value} value={opt.value}>{opt.name}</MenuItem>)}
              </Select>
            </FormControl>
          </div>
        </div>
      </Popover>
    </>
  )
}

export default React.memo(EditControl)
