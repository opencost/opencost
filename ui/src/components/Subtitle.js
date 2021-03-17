import React from 'react'
import { makeStyles } from '@material-ui/styles'
import { isArray, upperFirst } from 'lodash'
import Breadcrumbs from '@material-ui/core/Breadcrumbs'
import Link from '@material-ui/core/Link'
import NavigateNextIcon from '@material-ui/icons/NavigateNext'
import Tooltip from '@material-ui/core/Tooltip'
import Typography from '@material-ui/core/Typography'
import { toVerboseTimeRange } from '../util';

const useStyles = makeStyles({
  root: {
    '& > * + *': {
      marginTop: 2,
    },
  },
  link: {
    cursor: "pointer",
  },
})

const Subtitle = ({ report, context, clearContext, goToContext }) => {
  const classes = useStyles()

  const { aggregateBy, window } = report

  if (!isArray(context) || context.length === 0) {
    return (
      <div className={classes.root}>
        <Breadcrumbs
          separator={<NavigateNextIcon fontSize="small" />}
          aria-label="breadcrumb"
        >
          {aggregateBy && aggregateBy.length > 0 ? (
            <Typography>{toVerboseTimeRange(window)} by {upperFirst(aggregateBy)}</Typography>
          ) : (
            <Typography>{toVerboseTimeRange(window)}</Typography>
          )}
        </Breadcrumbs>
      </div>
    )
  }

  const handleBreadcrumbClick = (i, cb) => (e) => {
    e.preventDefault()
    cb(i)
  }

  return (
    <div className={classes.root}>
      <Breadcrumbs
        separator={<NavigateNextIcon fontSize="small" />}
        aria-label="breadcrumb"
      >
        <Link className={classes.link} color="inherit" onClick={() => clearContext()}>
          <Typography>{toVerboseTimeRange(window)} by {context[0].property}</Typography>
        </Link>
        {context.map((ctx, c) => {
          return c === context.length-1 ? (
            <Tooltip key={c} title={ctx.property} arrow>
              <Typography style={{ cursor: "default" }}>{ctx.name}</Typography>
            </Tooltip>
          ) : (
            <Link key={c} className={classes.link} color="inherit" onClick={handleBreadcrumbClick(c, goToContext)}>
              <Tooltip title={ctx.property} arrow>
                <Typography>{ctx.name}</Typography>
              </Tooltip>
            </Link>
          )
        })}
      </Breadcrumbs>
    </div>
  )
}

export default React.memo(Subtitle)
