import java.math.RoundingMode

/**
 * Name : EXT011MI.RoundOrderQty Version 1.0
 * Api to apply specific rules for rounding order qty
 *
 * Description :
 *
 * Date         Changed By    Description
 * 20221212     FLEBARS       Creation EXT010
 */
public class RoundOrderQty extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany

  public RoundOrderQty(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  /**
   * Initialize variables
   * Get Customer informations
   * Get Item informations
   * Get Item informations
   */
  public void main() {
    //Initializing vars
    currentCompany = (int)program.getLDAZD().CONO

    //Get API INPUTS
    String cuno = mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : ""
    String itno = mi.in.get("ITNO") != null ? (String)mi.in.get("ITNO") : ""
    double orqa = mi.in.get("ORQA") != null ? (Double)mi.in.get("ORQA") : 0d

    String a = "TEST"
    String b = new String("TEST")
    if (a == b){
      mi.error("a ${a} = b ${b}")
      return
    } else {
      mi.error("a ${a} != b ${b}")
      return
    }



    //Read customer Informations
    def dtaCustomer = getCustomerData(cuno)
    logger.debug(dtaCustomer.toString())
    if (dtaCustomer == null) {
      mi.error("Client ${cuno} n'existe pas")
      return
    }
    if (!"20".equals(dtaCustomer["STAT"])) {
      mi.error("Client ${cuno} n'est pas valide")
      return
    }
    //PAS D'ARRONDI
    if (!dtaCustomer["CHB7"].equals("1")) {
      mi.outData.put("ORQA", "" + orqa)
      mi.outData.put("REMK", "pas d'arrondi")
      mi.write()
      return
    }

    //Read Item data
    def dtaItem = getItemData(itno)
    if (dtaItem == null) {
      mi.error("Article ${itno} n'existe pas")
      return
    }
    if (!"20".equals(dtaItem["STAT"])) {
      mi.error("Article ${itno} n'est pas valide")
      return
    }





    def dtaGeneric = getGenericData()

    boolean rounded = false
    double outqty = 0d
    String remk = ""

    double gen_upa_sup = 0;
    double gen_upa_inf = 0;
    double gen_udp_sup = 0;
    double gen_udp_inf = 0;
    double gen_uco_sup = 0;
    double gen_uco_inf = 0;

    double rayon_upa_sup = 0;
    double rayon_upa_inf = 0;
    double rayon_udp_sup = 0;
    double rayon_udp_inf = 0;
    double rayon_uco_sup = 0;
    double rayon_uco_inf = 0;

    double cof_upa = 0;
    double cof_udp = 0;
    double cof_uco = 0;


    try {
      gen_upa_sup = Double.parseDouble(dtaGeneric["UPA_SUP"].toString())
    } catch (NumberFormatException e){}
    try {
      gen_upa_inf = Double.parseDouble(dtaGeneric["UPA_INF"].toString())
    } catch (NumberFormatException e){}
    try {
      gen_udp_sup = Double.parseDouble(dtaGeneric["UDP_SUP"].toString())
    } catch (NumberFormatException e){}
    try {
      gen_udp_inf = Double.parseDouble(dtaGeneric["UDP_INF"].toString())
    } catch (NumberFormatException e){}
    try {
      gen_uco_sup = Double.parseDouble(dtaGeneric["UCO_SUP"].toString())
    } catch (NumberFormatException e){}
    try {
      gen_uco_inf = Double.parseDouble(dtaGeneric["UCO_INF"].toString())
    } catch (NumberFormatException e){}
    try {
      rayon_upa_sup= Double.parseDouble(dtaItem["UPA_SUP"].toString())
    } catch (NumberFormatException e){}
    try {
      rayon_upa_inf= Double.parseDouble(dtaItem["UPA_INF"].toString())
    } catch (NumberFormatException e){}
    try {
      rayon_udp_sup= Double.parseDouble(dtaItem["UDP_SUP"].toString())
    } catch (NumberFormatException e){}
    try {
      rayon_udp_inf= Double.parseDouble(dtaItem["UDP_INF"].toString())
    } catch (NumberFormatException e){}
    try {
      rayon_uco_sup= Double.parseDouble(dtaItem["UCO_SUP"].toString())
    } catch (NumberFormatException e){}
    try {
      rayon_uco_inf= Double.parseDouble(dtaItem["UCO_INF"].toString())
    } catch (NumberFormatException e){}
    try {
      cof_upa= Double.parseDouble(dtaItem["COF_UPA"].toString())
    } catch (NumberFormatException e){}
    try {
      cof_udp= Double.parseDouble(dtaItem["COF_UDP"].toString())
    } catch (NumberFormatException e){}
    try {
      cof_uco= Double.parseDouble(dtaItem["COF_UCO"].toString())
    } catch (NumberFormatException e){}



    //
    for (int step = 0; step < 6; step++) {
      if (!rounded) {
        double lim_sup = 0;
        double lim_inf = 0;
        double cofa = 0;
        switch (step) {
          case 0:
            lim_sup = rayon_upa_sup
            lim_inf = rayon_upa_inf
            cofa = cof_upa
            remk = "rayon palette "
            break;
          case 1:
            lim_sup = rayon_udp_sup
            lim_inf = rayon_udp_inf
            cofa = cof_udp
            remk = "rayon 1/2 palette "
            break;
          case 2:
            lim_sup = rayon_uco_sup
            lim_inf = rayon_uco_inf
            cofa = cof_uco
            remk = "rayon couche "
            break;
          case 3:
            lim_sup = gen_upa_sup
            lim_inf = gen_upa_inf
            cofa = cof_upa
            remk = "global palette "
            break;
          case 4:
            lim_sup = gen_udp_sup
            lim_inf = gen_udp_inf
            cofa = cof_udp
            remk = "global 1/2 palette "
            break;
          case 5:
            lim_sup = gen_uco_sup
            lim_inf = gen_uco_inf
            cofa = cof_uco
            remk = "global couche "
            break;
          default:
            break;
        }
        logger.debug("step ${step} lim_sup ${lim_sup} lim_inf ${lim_inf} cofa ${cofa}"  )

        //Calcul
        if ((lim_sup > 0 || lim_inf > 0) && cofa > 0) {
          int nb_un = (int)(orqa / cofa)//Nb unit
          double reste = new BigDecimal(orqa - (nb_un * cofa)).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
          double preste = new BigDecimal(reste / cofa).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
          double lim_qty_sup = new BigDecimal(cofa * lim_sup / 100).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
          double lim_qty_inf = new BigDecimal(cofa * lim_inf / 100).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
          //logger.debug("step ${step} orqa ${orqa} cofa ${cofa} nb un ${nb_un}  reste ${reste} % ${preste} lim_qty_sup ${lim_qty_sup} lim_qty_inf ${lim_qty_inf}"  )
          logger.debug("step=${step},remk=${remk},reste=${reste},%reste=${preste},lim_sup=${lim_sup},lim_inf=${lim_inf}")

          if (preste >= lim_sup && reste != 0) {
            rounded = true
            outqty = new BigDecimal((nb_un + 1) * cofa).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
            remk += " > " + (lim_sup * 100) + "%"
          } else if (preste < lim_inf && reste != 0) {
            rounded = true
            outqty = new BigDecimal((nb_un) * cofa).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
            remk += " < " + (lim_inf * 100) + "%"
          } else if (reste == 0) {
            rounded = true
            outqty = orqa
            remk += " ="
          }
        }
      }
    }

    if (!rounded) {
      mi.outData.put("ORQA", "" + orqa)
      mi.outData.put("REMK", "pas d'arrondi")
      mi.write()
    } else {
      mi.outData.put("ORQA", "" + outqty)
      mi.outData.put("REMK", remk)
      mi.write()
    }
  }

  /**
   * Get Customer Informations from OCUSMA and CUGEX1
   * @param cuno Customer
   * @return null if not found else structure with STAT, CHB7
   */
  private def getCustomerData(String cuno) {
    def responseObject = [
      "STAT" : "",
      "CHB7" : ""
    ]

    DBAction queryOCUSMA00 = database.table("OCUSMA").index("00").selection(
        "OKCONO",
        "OKCUNO",
        "OKSTAT"
        ).build()

    DBContainer containerOCUSMA = queryOCUSMA00.getContainer()
    containerOCUSMA.set("OKCONO",currentCompany)
    containerOCUSMA.set("OKCUNO", cuno)
    if (queryOCUSMA00.read(containerOCUSMA)) {
      responseObject["STAT"] = containerOCUSMA.get("OKSTAT").toString()
    } else {
      return null
    }

    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection(
        "F1CONO",
        "F1FILE",
        "F1PK01",
        "F1PK02",
        "F1PK03",
        "F1PK04",
        "F1PK05",
        "F1PK06",
        "F1PK07",
        "F1PK08",
        "F1CHB7"
        ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "OCUSMA")
    containerCUGEX1.set("F1PK01", cuno)

    if (queryCUGEX100.read(containerCUGEX1)) {
      responseObject["CHB7"] = containerCUGEX1.get("F1CHB7").toString()
    }
    return responseObject
  }

  /**
   * Get Item Informations from MITMAS, 
   *  CUGEX1.MITHRY k01=2, k02=mitmas.hie2 
   *  MITAUN
   * @param itno Item 
   * @return null if not found else structured object fields STAT, HIE2, UPA_SUP, UPA_INF, UDP_SUP, UDP_INF, UCO_SUP, UCO_INF, COF_UPA, COF_UDP, COF_UCO
   */
  private def getItemData(String itno) {
    //Define return object structure
    def responseObject = [
      "STAT" : "",
      "HIE2" : "",
      "UPA_SUP" : "",
      "UPA_INF" : "",
      "UDP_SUP" : "",
      "UDP_INF" : "",
      "UCO_SUP" : "",
      "UCO_INF" : "",
      "COF_UPA" : "",
      "COF_UDP" : "",
      "COF_UCO" : ""
    ]

    String hie2 = ""


    DBAction queryMITMAS00 = database.table("MITMAS").index("00").selection(
        "MMCONO",
        "MMITNO",
        "MMSTAT",
        "MMHIE2"
        ).build()

    DBContainer containerMITMAS = queryMITMAS00.getContainer()
    containerMITMAS.set("MMCONO",currentCompany)
    containerMITMAS.set("MMITNO", itno)
    if (queryMITMAS00.read(containerMITMAS)) {
      responseObject["STAT"] = containerMITMAS.get("MMSTAT").toString()
      hie2 = containerMITMAS.getString("MMHIE2")
      responseObject["HIE2"] = hie2
    } else {
      return null
    }


    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection(
        "F1CONO",
        "F1FILE",
        "F1PK01",
        "F1PK02",
        "F1PK03",
        "F1PK04",
        "F1PK05",
        "F1PK06",
        "F1PK07",
        "F1PK08",
        "F1N296",
        "F1N596",
        "F1N396",
        "F1N696",
        "F1N496",
        "F1N796"
        ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "MITHRY")
    containerCUGEX1.set("F1PK01", "2")
    containerCUGEX1.set("F1PK02", hie2)

    if (queryCUGEX100.read(containerCUGEX1)) {
      responseObject["UPA_SUP"] =  containerCUGEX1.get("F1N296").toString()
      responseObject["UPA_INF"] =  containerCUGEX1.get("F1N596").toString()
      responseObject["UDP_SUP"] =  containerCUGEX1.get("F1N396").toString()
      responseObject["UDP_INF"] =  containerCUGEX1.get("F1N696").toString()
      responseObject["UCO_SUP"] =  containerCUGEX1.get("F1N496").toString()
      responseObject["UCO_INF"] =  containerCUGEX1.get("F1N796").toString()
    }

    DBAction queryMITAUN00 = database.table("MITAUN").index("00").selection(
        "MUCONO",
        "MUITNO",
        "MUAUTP",
        "MUALUN",
        "MUCOFA",
        "MUDMCF"
        ).build()

    DBContainer containerMITAUN = queryMITAUN00.getContainer()
    containerMITAUN.set("MUCONO", currentCompany)
    containerMITAUN.set("MUITNO", itno)
    containerMITAUN.set("MUAUTP", 1)

    Closure<?> readMITAUN = { DBContainer resultMITAUN ->
      String alun = resultMITAUN.getString("MUALUN").toString()
      double cofa = resultMITAUN.getDouble("MUCOFA")
      String dmcf = resultMITAUN.getInt("MUDMCF")

      cofa = "2".equals(dmcf) ? 1 / cofa : cofa
      cofa = new BigDecimal(cofa).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      if ("UPA".equals(alun)) {
        responseObject["COF_UPA"] = "" + cofa
      } else if ("UDP".equals(alun)){
        responseObject["COF_UDP"] = "" + cofa
      } else if ("UCO".equals(alun)){
        responseObject["COF_UCO"] = "" + cofa
      }
    }
    queryMITAUN00.readAll(containerMITAUN, 3, readMITAUN)
    logger.debug(responseObject.toString())

    return responseObject
  }

  /**
   * Get generic rounding paramaters
   * in CUGEX1.OFREEF k01 MITMAS k02 FR
   * @return null if not found else structured object fields 
   *     UPA_SUP, 
   *     UPA_INF, 
   *     UDP_SUP, 
   *     UDP_INF, 
   *     UCO_SUP, 
   *     UCO_INF
   */
  private def getGenericData() {
    //Define return object structure
    def responseObject = [
      "UPA_SUP" : "",
      "UPA_INF" : "",
      "UDP_SUP" : "",
      "UDP_INF" : "",
      "UCO_SUP" : "",
      "UCO_INF" : ""
    ]
    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection(
        "F1CONO",
        "F1FILE",
        "F1PK01",
        "F1PK02",
        "F1PK03",
        "F1PK04",
        "F1PK05",
        "F1PK06",
        "F1PK07",
        "F1PK08",
        "F1N296",
        "F1N596",
        "F1N396",
        "F1N696",
        "F1N496",
        "F1N796"
        ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "OFREEF")
    containerCUGEX1.set("F1PK01", "MITMAS")
    containerCUGEX1.set("F1PK02", "FR")

    if (queryCUGEX100.read(containerCUGEX1)) {
      responseObject["UPA_SUP"] =  containerCUGEX1.get("F1N296").toString()
      responseObject["UPA_INF"] =  containerCUGEX1.get("F1N596").toString()
      responseObject["UDP_SUP"] =  containerCUGEX1.get("F1N396").toString()
      responseObject["UDP_INF"] =  containerCUGEX1.get("F1N696").toString()
      responseObject["UCO_SUP"] =  containerCUGEX1.get("F1N496").toString()
      responseObject["UCO_INF"] =  containerCUGEX1.get("F1N796").toString()
    }
    return responseObject
  }
}