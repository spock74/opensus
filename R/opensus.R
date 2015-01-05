#'Obtem todo o documento mongodb relativo a um dado numero CNES
#'@param cnesCod Codigo CNES a ser pesquisado
#'@author Jose Moraes
getDocByCnes <- function(cnesCod, ns = "opensus.cnesminas"){
    # Variavel de saida
    res <- NULL
    # Conecta com mongodb
    mongo <- rmongodb::mongo.create()
    query <- rmongodb::mongo.bson.from.list( list(CNES = cnesCod) )
    # res é da classe R list
    res <- rmongodb::mongo.find.batch(mongo, ns = ns, query, limit=100L)
    # Fechar recursos
    rmongodb::mongo.disconnect(mongo)
    rmongodb::mongo.destroy(mongo)
    
    return(res)
}

#' Obtem o numero de resistros (documentos na instancia do mongodb)
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
getLatLonByMunicipio <- function(municipio, ns  = "opensus.cnesminas", nLimit = 30000L){
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
    
    ## Incia mapa
    
    
    
    
    return( res )
}

#'@author Jose Moraes
#'@title Nao usar
#'
#'
#'
# navegarApp <- function(porta){
#     opencpu::opencpu$stop()
#     opencpu::opencpu$start(porta)
#     opencpu::opencpu$url()
#     opencpu::opencpu$browse("/library/opensus/www")
# }



#'@title Nao usar
#'@author Jose Moraes
#'
#'
#'
#'
# navegarTeREST <- function(){
#     opencpu::opencpu$stop()
#     opencpu::opencpu$start(4242)
#     opencpu::opencpu$url()
#     opencpu::opencpu$browse("/library/TeREST/www")
# }


