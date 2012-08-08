##                                                  _                        ##
##   ____ _____ ____   ____    ____ ____ ____  ____| |_  ___   ____          ##
##  / ___|___  )    \ / _  |  / ___) _  ) _  |/ ___)  _)/ _ \ / ___)         ##
## | |    / __/| | | | | | |_| |  ( (/ ( ( | ( (___| |_| |_| | |             ##
## |_|   (_____)_|_|_|\_|| (_)_|   \____)_||_|\____)\___)___/|_|             ##
##                       |_| The event reactor for ZeroMQ sockets in R.      ##
##                              Copyright © 2012, James Brotchie             ##

library(rzmq)

`zmq.reactor` <- function() {
    reactor <- new.env()
    class(reactor) <- 'zmq.reactor'

    reactor$running <- FALSE
    reactor$socket.handlers <- list()

    reactor
}

`stop.reactor` <- function(reactor) {
    if (!reactor$running) stop('Reactor not running.')
    reactor$running <- FALSE
}

`run.reactor` <- function(reactor) {
    if (reactor$running) stop('Reactor already running.')
    reactor$running <- TRUE

    while(reactor$running) {
        event.flags <- poll.reactor(reactor)
        mapply(function(handler, flags) {
                   flag.set <- function (flags, event) !is.null(flags[[event]]) && flags[[event]] == TRUE

                   if (flag.set(flags, 'read'))  handler$on.read(handler)
                   if (flag.set(flags, 'write')) handler$on.write(handler)
                   if (flag.set(flags, 'error')) handler$on.error(handler)

               }, reactor$socket.handlers, event.flags)
    }
}

`field` <- function(name) function(x) get(name, x)
`get.fields` <- function(items, name) lapply(items, field(name))
`not.null.mask` <- function(items) unlist(lapply(items, Negate(is.null)))
`required.events` <- function(handler) {
    required <- not.null.mask(list(handler$on.read, handler$on.write, handler$on.error))
    c('read', 'write', 'error')[required]
}

`poll.reactor` <- function(reactor) {
    sockets <- get.fields(reactor$socket.handlers, 'socket')
    events <- lapply(reactor$socket.handlers, required.events)
    poll.socket(sockets, events, timeout=0L)
}

`zmq.socket.handler` <- function(socket, on.read=NULL,
                                 on.write=NULL, on.error=NULL) {
    handler <- new.env()
    class(handler) <- 'zmq.socket.handler'

    handler$socket   <- socket
    handler$on.read  <- on.read
    handler$on.write <- on.write
    handler$on.error <- on.error

    handler$receive <- function(...) receive.socket(handler$socket, ...)
    handler$send    <- function(...) send.socket(handler$socket, ...)

    handler
}

reactor <- zmq.reactor()
ctx <- init.context()

sreq <- init.socket(ctx, 'ZMQ_REQ')
srep <- init.socket(ctx, 'ZMQ_REP')

bind.socket(srep, 'ipc:///var/tmp/rtest')
connect.socket(sreq, 'ipc:///var/tmp/rtest')

count <- 5

srep.handler <- zmq.socket.handler(srep,
                                   on.read = function(x) {
                                       print("REP read")
                                       print(x$receive())
                                   },
                                   on.write = function(x) {
                                       print("REP write")
                                       x$send('World')
                                   })

sreq.handler <- zmq.socket.handler(sreq,
                                   on.read = function(x) {
                                       print("REQ read")
                                       print(x$receive())
                                       if (count == 0) {
                                           stop.reactor(reactor)
                                       } else {
                                           print(paste(count, 'remaining.'))
                                           count <<- count - 1
                                       }
                                   },
                                   on.write = function(x) {
                                       print('REQ write')
                                       x$send('Hello')
                                   })
reactor$socket.handlers <- list(srep.handler, sreq.handler)

run.reactor(reactor)
