mkRow <- function(nCol) {
    x <- as.list(data.frame(UserId = as.integer(runif(1, min = 1, max = 1.6e7)),
                            Country = "FR",
                            ArtistId = as.integer(runif(1, min = 1, max = 2e5)),
                            TrackId = as.integer(runif(1, min = 1, max = 3.5e7))))
    # make row mixed types by changing first column to string
    names(x) <- paste('x',seq_len(nCol),sep='.')
    x
}

mkFrameInPlace <- function(nRow,nCol,classHack=TRUE) {
    r1 <- mkRow(nCol)
    d <- data.frame(r1,
                    stringsAsFactors=FALSE)
    if(nRow>1) {
        d <- d[rep.int(1,nRow),]
        if(classHack) {
            # lose data.frame class for a while
            # changes what S3 methods implement
            # assignment.
            d <- as.list(d) 
        }
        for(i in seq.int(2,nRow,1)) {
            ri <- mkRow(nCol)
            for(j in seq_len(nCol)) {
                d[[j]][i] <- ri[[j]]
            }
        }
    }
    if(classHack) {
        d <- data.frame(d,stringsAsFactors=FALSE)
    }
    d
}

fakeData = mkFrameInPlace(nRow = 100000, nCol = 4)

