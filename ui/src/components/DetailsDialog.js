import React from 'react'
import { get } from 'lodash'
import Button from '@material-ui/core/Button'
import Dialog from '@material-ui/core/Dialog'
import DialogActions from '@material-ui/core/DialogActions'
import DialogContent from '@material-ui/core/DialogContent'
import DialogTitle from '@material-ui/core/DialogTitle'
import Details from './Details'

const DetailsDialog = ({
  open,
  close,
  details,
  window,
  currency,
}) => {
  const namespace = get(details, 'namespace', '')
  const controllerKind = get(details, 'controllerKind', '')
  const controller = get(details, 'controller', '')
  const pod = get(details, 'pod', '')

  let title = controller
  if (namespace) {
    title = `${namespace} / ${title}`
  }
  if (pod) {
    title = `${title} : ${pod}`
  }

  return (
    <Dialog
      open={open}
      onClose={close}
      maxWidth="lg"
      fullWidth
    >
      <DialogTitle>{title}</DialogTitle>
      <DialogContent>
        {(controllerKind && controller) &&
          <Details
            window={window}
            currency={currency}
            namespace={namespace}
            controllerKind={controllerKind}
            controller={controller}
            pod={pod}
          />
        }
      </DialogContent>
      <DialogActions>
        <Button onClick={close} color="primary">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export default DetailsDialog
