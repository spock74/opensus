#'Retrieve the \code{mongodb document} given a CNES code number
#'@param cnesCod Codigo CNES a ser pesquisado
#'@param ns \code{mongodb} namespace 
#'@author Jose Moraes
getDocByCnes <- function(cnesCod, ns = "opensus.cnesminas"){
  res <- NULL
  # Gets a mongodb connection instance
  mongo <- rmongodb::mongo.create()
  #TODO chek for errors, assert connection properties are ok
  
  query <- rmongodb::mongo.bson.from.list( list(CNES = cnesCod) )
  #TODO chek for errors, assert query properties/metadata are ok
  
  # Get an R list object containing result.
  res <- rmongodb::mongo.find.batch(mongo, ns = ns, query, limit = 100L)
  #TODO chek for errors, assert metada data are as expected, like having 
  # exactly one element in the list (TODO remember check with G M Cunha if
  # CNES code are UNIQUE identifiers)
  
  # Free resources
  rmongodb::mongo.disconnect(mongo)
  rmongodb::mongo.destroy(mongo)
  #TODO check for errors and or warnings, treat them accordingly 
  
  return(res)
}


#'Convenience method to print the fields contained in \code{mongodb document} 
#'as a dataframe. Namespace is hard coded as \code{ns = "opensus.cnesminas"}
#'@return \code{mongodb document}
#'@author Jose Moraes
getDocStructure <- function(){
  res <- NULL
  # Gets a mongodb connection instance
  mongo <- rmongodb::mongo.create()
  #TODO chek for errors, assert connection properties are ok

  # Get R list object
  if(rmongodb::mongo.is.connected(mongo)){
    res <- rmongodb::mongo.find.batch(mongo, ns = ns, query, limit = 100L)
    #TODO chek for errors, assert metada data are as expected    
    
  }else{
    print("Unable to connect.  Error code: ")
    print( mongo.get.err(mongo) )
  }

  #Just pick a document based on a given field (UF, here)
  cursor <- rmongodb::mongo.find.one(mongo = mongo, 
                                    ns = "opensus.cnesminas", 
                                    query = mongo.bson.from.list(UF = "MG"),
                                    fields = mongo.bson.empty())
  #TODO chek for errors, asserts that query properties/metadata are ok
  
  res <- rmongodb::mongo.cursor.to.data.frame(cursor = cursor)
  
  
  
  # Free resources
  if( !rmongodb::mongo.is.connected(mongo) ){
    rmongodb::mongo.get.last.err(mongo = mongo, db = "opensus")
  }
  rmongodb::mongo.disconnect(mongo)
  rmongodb::mongo.destroy(mongo)
  #TODO check for errors and or warnings, treat them accordingly 
  
  return(res)
}

#' Obtem o numero de registros (documentos na instancia do mongodb)
#' @param municipio nome do municipio
#' @param ns namespace a ser usado, na forma 'database.collection'
#' @author Jose Moraes
#'
getNumDocByMunicipio <- function(municipio, ns = "opensus.cnesminas"){
    
    res <- NULL
    mongo <- rmongodb::mongo.create()
    query <- rmongodb::mongo.bson.from.list( list(Municipio = toupper(municipio)) )
    res <- rmongodb::mongo.count(mongo = mongo, ns = ns, query)
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    return(res)
}

#' Obtem um objeto da classe \code{R List} com todos os 
#' campos existentes no documentos \code{mongodb}
#' @param cnesCod Codigo CNES a ser pesquisado
#' @param nLimite numero maximo de documentos a ser retornado, padrao 10.
#' @param ns \code{namespace} na instancia \code{mongodb} a ser usado na pesquisa
#' @author Jose Moraes
getDocByMunicipio <- function(municipio, ns = "opensus.cnesminas", nLimite = 10L){
    
    res <- NULL
    mongo <- rmongodb::mongo.create()
    query <- rmongodb::mongo.bson.from.list( list(Municipio = toupper(municipio)) )
    res <- rmongodb::mongo.find.batch(mongo = mongo, ns = ns, query, limit = nLimite)
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    return(res)
}

#'TODO
#'
#'
#'
#'
#'
#'
getGeoJsonByMunicipio <- function(){
    print("YTBD")
    return("YTBD")
}

#' Retrive a dataframe containing Latitud and Longitude for a given CNES cod
#'@author Jose Moraes
#'@param cnesCod CNES number of a institution
#'
#'
#'@return res a data frame with values for Lat and Lon
getLatLonByCnes <- function(cnesCod, ns  = "opensus.cnesminas"){
    
    res <- NULL
    # connect to your local mongodb
    mongo <- rmongodb::mongo.create()
    
    # create query object 
    query <- rmongodb::mongo.bson.from.list( list(CNES = cnesCod) )
    
    res <- rmongodb::mongo.find.batch(mongo = mongo, ns  = ns, query, limit=1L)
    
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    return( data.frame( Lat = res[[1]]$Latitude, Lon = res[[1]]$Longitude) )
}


#'Retrieve a data.frame containing all Latitude and Longitude 
#'values for a given municipio name
#'@author Jose Moraes
#'@param municipio name of the municipio
#'
#'
#'@return res a data.frame with values for Lat and Lon
getLatLonByMunicipio <- function(municipio, ns  = "opensus.cnesminas", nLimit = 100L){
    res <- NULL
    
    # connect to local mongodb
    mongo <- rmongodb::mongo.create()
    # create bson query object 
    query <- rmongodb::mongo.bson.from.list( list(Municipio = toupper(municipio)) )
    res <- rmongodb::mongo.find.batch(mongo = mongo, ns  = ns, query, limit = nLimit)
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    
    res.df <- data.frame(CNES = rep(NA, length(res)), 
                         Lat = rep(NA, length(res)), 
                         Lon = rep(NA, length(res)))
    
    for(ii in 1:length(res)){
        res.df$CNES[ii] <- res[[ii]]$CNES
        res.df$Lat[ii]  <- res[[ii]]$Latitude
        res.df$Lon[ii]  <- res[[ii]]$Longitude
    }
    
    return( res.df )
}

#'Get Fields values based a R named list in te form fieldsList = ("Key" = "value" )
#'@param fieldsList a R named list containing ("key" = "VALUE") pairs 
#'@param nLimit 
#'@param ns
#'@author Jose Moraes
#'
getDocByFieldsList <- function(fieldsList, ns  = "opensus.cnesminas", nLimit = 10L){
    
    res <- NULL
    mongo <- rmongodb::mongo.create()
    
    # create query object 
    query <- rmongodb::mongo.bson.from.list( fieldsList )
    
    res <- rmongodb::mongo.find.batch(mongo = mongo, ns  = ns, query, limit = nLimit)
    
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    return( res )
}

#'Get Fields values based a R named list in te form fieldsList = ("Key" = "value" )
#'@param fieldsList a R named list containing ("key" = "VALUE") pairs 
#'@param nLimit 
#'@param ns
#'@author Jose Moraes
#'
getMapByMunicipioList <- function(municipio, mapsource = "osm", ns  = "opensus.cnesminas", nLimit = 10L){
    
    res <- NULL
    mongo <- rmongodb::mongo.create()
    
    # create query object 
    query <- rmongodb::mongo.bson.from.list( list(Municipio = toupper(municipio)) )
    
    res <- rmongodb::mongo.find.batch(mongo = mongo, ns  = ns, query, limit = nLimit)
    
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)

    return( res )
}

#'TODOTODOTODOTODOTODOTODOTODO
#'Retrieve a data.frame containing all Latitude and Longitude 
#'values for a given municipio name
#'@author Jose Moraes
#'@param municipio name of the municipio
#'
#'
#'@return res a data.frame with values for Lat and Lon
getLatLonByMunicipio <- function(municipio, ns  = "opensus.cnesminas", nLimit = 100L){
    res <- NULL
    
    # connect to local mongodb
    mongo <- rmongodb::mongo.create()
    # create bson query object 
    query <- rmongodb::mongo.bson.from.list( list(Municipio = toupper(municipio)) )
    res <- rmongodb::mongo.find.batch(mongo = mongo, ns  = ns, query, limit = nLimit)
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    
    res.df <- data.frame(CNES = rep(NA, length(res)), 
                         Lat = rep(NA, length(res)), 
                         Lon = rep(NA, length(res)))
    
    for(ii in 1:length(res)){
        res.df$CNES[ii] <- res[[ii]]$CNES
        res.df$Lat[ii]  <- res[[ii]]$Latitude
        res.df$Lon[ii]  <- res[[ii]]$Longitude
    }
    
    return( res.df )
}

#'geoJSON representation of a given municipio \code{string} name and a 
#'\code{integer} to limit number os records returned
#'@param municipio municipio name in \code{string} format
#'@param nLimit maximum number of records to br returned
#'@param ns namespace of \code{mongodb} instance default: "opensus.cnesminas"
#'@author Jose Moraes
#'
getGeoJSONByMunicipio <- function(municipio = "Vargem Alegre", ns  = "opensus.cnesminas", nLimit = 10L){
    
    res <- NULL
    mongo <- rmongodb::mongo.create()
    
    # create query object 
    query <- rmongodb::mongo.bson.from.list( list(Municipio = toupper(municipio)) )
    
    res <- rmongodb::mongo.find.batch(mongo = mongo, ns  = ns, query, limit = nLimit)
    
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    res.df <- data.frame(CNES = rep(NA, length(res)), 
                         Lat = rep(NA, length(res)), 
                         Lon = rep(NA, length(res)))
    
    for(ii in 1:length(res)){
        res.df$CNES[ii] <- res[[ii]]$CNES
        res.df$Lat[ii]  <- res[[ii]]$Latitude
        res.df$Lon[ii]  <- res[[ii]]$Longitude
    }
    
    #geoJ <- leafletR::toGeoJSON(data = res.df, lat.lon = c(res.df$Lat, res.df$Lon) )
    
    return( res.df )
}



#' Map
#'
#'
#'
#'
getMapByMunicipio <- function(municipio = "Belo Horizonte", ns  = "opensus.cnesminas", nLimit = 10L){
    
    
    res <- NULL
    mongo <- rmongodb::mongo.create()
    
    # create query object 
    query <- rmongodb::mongo.bson.from.list( list(Municipio = toupper(municipio)) )
    
    res <- rmongodb::mongo.find.batch(mongo = mongo, ns  = ns, query, limit = nLimit)
    
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    res.df <- data.frame(CNES = rep(NA, length(res)), 
                         Lat = rep(NA, length(res)), 
                         Lon = rep(NA, length(res)))
    
    for(ii in 1:length(res)){
        res.df$CNES[ii] <- res[[ii]]$CNES
        res.df$Lat[ii]  <- res[[ii]]$Latitude
        res.df$Lon[ii]  <- res[[ii]]$Longitude
    }
    
    tmarkers <- data.frame(lon = res.df$Lon, lat = res.df$Lat)
    
    map <- ggmap::get_googlemap(center = c(lon = res.df$Lon[1], res.df$Lat[1]),
                         zoom = 10, size = c(640, 640), scale = 2, markers = tmarkers)
    
    return( map )
}
