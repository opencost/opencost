import React from 'react'
import { makeStyles } from '@material-ui/styles'
import Breadcrumbs from '@material-ui/core/Breadcrumbs';
import Link from '@material-ui/core/Link';
import Typography from '@material-ui/core/Typography';

const useStyles = makeStyles({
  root: {
    alignItems: 'center',
    display: 'flex',
    flexFlow: 'row',
    marginBottom: 20,
    width: '100%',
  },
  context: {
    flex: '1 0 auto',
  },
  actions: {
    flex: '0 0 auto',
  },
});

const Header = (props) => {
  const classes = useStyles()
  const { title, breadcrumbs } = props

  return (
    <div className={classes.root}>
      <img src={ require('../images/logo.png') } alt="OpenCost" />
      <div className={classes.context}>
        {title && <Typography variant="h4" className={classes.title}>{props.title}</Typography>}
        {breadcrumbs && breadcrumbs.length > 0 && (
          <Breadcrumbs aria-label="breadcrumb">
            {breadcrumbs.slice(0, breadcrumbs.length-1).map(b => (
              <Link color="inherit" href={b.href} key={b.name}>{b.name}</Link>
            ))}
            <Typography color="textPrimary">{breadcrumbs[breadcrumbs.length-1].name}</Typography>
          </Breadcrumbs>
        )}
      </div>
      <div className={classes.actions}>
        {props.children}
      </div>
    </div>
  )
}

export default Header
