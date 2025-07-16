/**
 * Name : EXT012MI.SelSupplyChain
 *
 * Description :
 * This API method is used to get more infos from MWS150MI SelSupplyChain
 *
 *
 * Date         Changed By    Description
 * 20240208     MLECLERCQ     APP02 - Planning GDA
 */

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

    String ridnInput = (mi.in.get("RIDN")).toString()
    String orcaInput = (mi.in.get("ORCA")).toString()
    Integer ponrInput = (mi.in.get("PONR"))
    String ridsInput = (mi.in.get("RIDS")).toString()
    String scnbInput = (mi.in.get("SCNB")).toString()
    String orc2Input = (mi.in.get("ORC2")).toString()
    String suclInput = (mi.in.get("SUCL")).toString()

    if (ridnInput == null || ridnInput == "") {
      mi.error("N° de commande obligatoire.")
      return
    }

    if (orcaInput == null || orcaInput == "") {
      mi.error("Catégorie d'ordre obligatoire.")
      return
    }

    if (orc2Input == null || orc2Input == "") {
      mi.error("Catégorie d'ordre secondaire est obligatoire")
      return
    }

    if (suclInput == null || suclInput == "") {
      mi.error("Groupe fournisseur est obligatoire")
      return
    }

    if(ponrInput == null){
      ponrInput = null
    }

    if(ridsInput == null){
      ridsInput = ""
    }

    if(scnbInput == null){
      scnbInput = ""
    }

    logger.debug('Received RIDN input : ' + ridnInput)
    logger.debug('Received ORCA input : ' + orcaInput)
    logger.debug('Received ORC2 input : ' + orc2Input)
    logger.debug('Received SUCL input : ' + suclInput)

    Map<String, String> params = [
      "RIDN": ridnInput,
      "ORCA": orcaInput
    ]

    if(ponrInput){
      logger.debug("ponrInput as string : " + ponrInput.toString())
      params.put("PONR", ponrInput.toString())
    }

    Closure<?> closureMWS150 = {
      Map<String, String> response ->
        logger.debug("Response = ${response}")

        logger.debug('Received RIDN from MWS150MI:' + (String) response["RIDN"])
        logger.debug('Received ORCA from MWS150MI:' + (String) response["ORCA"])
        logger.debug('Received ITNO from MWS150MI:' + (String) response["ITNO"])
        logger.debug('Received WHLO from MWS150MI:' + (String) response["WHLO"])
        logger.debug('Received PONR from MWS150MI:' + (String) response["PONR"])

        Boolean valid = false

        if(ponrInput){
          valid = true
        }else if((String) response["ORCA"] == orc2Input){
          valid = true
        }

        if (valid) {
          String itno = (String) response["ITNO"].toString()
          String ridn = (String) response["RIDN"].toString()
          String ponr = (String) response["PONR"].toString()
          String rids = (String) response["RIDS"].toString()
          String whlo = (String) response["WHLO"].toString()

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
                logger.debug("CIDVEN sucl = " + cidvenContainer.get("IISUCL").toString() + ", input sucl :" + suclInput)
                if(cidvenContainer.get("IISUCL") == suclInput){
                  logger.debug("Record passed all conditions with SUNO : " + cidvenContainer.get("IISUNO") +", and SUCL : " + cidvenContainer.get("IISUCL") )

                  mi.outData.put("RIDN", ridn)
                  mi.outData.put("PONR", ponr)
                  mi.outData.put("ITNO", itno)
                  mi.outData.put("RIDS", rids)
                  mi.outData.put("WHLO", whlo)
                  mi.outData.put("SUNO", suno)

                  mi.write()

                }
              }
            }

          }

        }


    }

    miCaller.setListMaxRecords(9999)
    miCaller.call("MWS150MI", "SelSupplyChain", params, closureMWS150)
  }
}
