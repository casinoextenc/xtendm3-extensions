public class SelSupplyChain extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany



  public SelSupplyChain(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility,MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    currentCompany = (Integer) program.getLDAZD().CONO
    int maxReturnedRecords = 9999

    String ridn_input = (mi.in.get("RIDN")).toString()
    String orca_input = (mi.in.get("ORCA")).toString()
    Integer ponr_input = (mi.in.get("PONR"))
    String rids_input = (mi.in.get("RIDS")).toString()
    String scnb_input = (mi.in.get("SCNB")).toString()
    String orc2_input = (mi.in.get("ORC2")).toString()
    String sucl_input = (mi.in.get("SUCL")).toString()

    if (ridn_input == null || ridn_input == "") {
      mi.error("N° de commande obligatoire.")
      return
    }

    if (orca_input == null || orca_input == "") {
      mi.error("Catégorie d'ordre obligatoire.")
      return
    }

    if (orc2_input == null || orc2_input == "") {
      mi.error("Catégorie d'ordre secondaire est obligatoire")
      return
    }

    if (sucl_input == null || sucl_input == "") {
      mi.error("Groupe fournisseur est obligatoire")
      return
    }

    if(ponr_input == null){
      ponr_input = null
    }

    if(rids_input == null){
      rids_input = ""
    }

    if(scnb_input == null){
      scnb_input = ""
    }

    logger.debug('Received RIDN input : ' + ridn_input)
    logger.debug('Received ORCA input : ' + orca_input)
    logger.debug('Received ORC2 input : ' + orc2_input)
    logger.debug('Received SUCL input : ' + sucl_input)

    Map<String, String> params = [
      "RIDN": ridn_input,
      "ORCA": orca_input
    ]

    if(ponr_input){
      logger.debug("ponr_input as string : " + ponr_input.toString())
      params.put("PONR", ponr_input.toString())
    }

    Closure<?> closureMWS150 = {
      Map<String, String> response ->
        logger.debug("Response = ${response}")

        logger.debug('Received RIDN from MWS150MI:' + (String) response["RIDN"])
        logger.debug('Received ORCA from MWS150MI:' + (String) response["ORCA"])
        logger.debug('Received ITNO from MWS150MI:' + (String) response["ITNO"])
        logger.debug('Received WHLO from MWS150MI:' + (String) response["WHLO"])
        logger.debug('Received PONR from MWS150MI:' + (String) response["PONR"])

        Boolean valid = false;

        if(ponr_input){
          valid = true
        }else if((String) response["ORCA"] == orc2_input){
          valid = true
        }

        if (valid) {
          String itno = (String) response["ITNO"].toString()
          String ridn = (String) response["RIDN"].toString()
          String ponr = (String) response["PONR"].toString()
          String rids = (String) response["RIDS"].toString()
          String whlo = (String) response["WHLO"].toString()


          //DBAction rechercheMPOPLP = database.table("MPOPLP").index("00").matching(expression).selection("POPLPN","POITNO","POSUNO").build()
          DBAction rechercheMPOPLP = database.table("MPOPLP").index("00").selection("POPLPN","POITNO","POSUNO").build()
          DBContainer mpoplpContainer = rechercheMPOPLP.createContainer()
          mpoplpContainer.set("POCONO", currentCompany)
          mpoplpContainer.set("POPLPN", response["RIDN"] as Integer)

          if(rechercheMPOPLP.read(mpoplpContainer)){

            if(mpoplpContainer.get("POITNO") == itno){
              logger.debug("MPOPLP retrieved : PLPN = " + mpoplpContainer.get('POPLPN').toString())
              logger.debug("MPOPLP retrieved : ITNO = " + mpoplpContainer.get('POITNO').toString())
              logger.debug("MPOPLP retrieved : SUNO = " + mpoplpContainer.get('POSUNO').toString())

              String suno = mpoplpContainer.get('POSUNO').toString()

              DBAction rechercheCIDVEN = database.table("CIDVEN").index("00").selection("IISUNO","IISUCL").build()
              DBContainer cidvenContainer = rechercheCIDVEN.createContainer()
              cidvenContainer.set("IICONO", currentCompany)
              cidvenContainer.set("IISUNO", suno)

              if(rechercheCIDVEN.read(cidvenContainer)){
                logger.debug("CIDVEN sucl = " + cidvenContainer.get("IISUCL").toString() + ", input sucl :" + sucl_input)
                if(cidvenContainer.get("IISUCL") == sucl_input){
                  logger.debug("Record passed all conditions with SUNO : " + cidvenContainer.get("IISUNO") +", and SUCL : " + cidvenContainer.get("IISUCL") )

                  mi.outData.put("RIDN", ridn)
                  mi.outData.put("PONR", ponr)
                  mi.outData.put("ITNO", itno)
                  mi.outData.put("RIDS", rids)
                  mi.outData.put("RIDN", ridn)
                  mi.outData.put("WHLO", whlo)
                  mi.outData.put("SUNO", suno)

                  mi.write()

                }
              }
            }

          }

        }


    }

    miCaller.call("MWS150MI", "SelSupplyChain", params, closureMWS150)
  }
}
