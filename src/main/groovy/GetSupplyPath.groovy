import java.math.RoundingMode

/**
 * Name : EXT011MI.GetSupplyPath Version 1.0
 *
 * Description :
 * The API retrieve the supply path of an item according to the input parameters
 *  customer , order type, POPN, warehouse and Qty ordered.
 *
 * The decision process is iterative,
 * The api has an input flag that indicate whether
 *  the response should contain all the steps that made the decision possible
 *  or only the last valid step from which to create the order line
 *
 * The response contains a flag that indicate which line is valid. It also contains a remark field to justify each step.
 *
 * In complete mode (mi.in.get("FLAG")="1")
 *  If the search is successful, only the last line of the answer is flagged true.
 *  If the search is unsuccessful, all the answer lines are flagged false.
 * In other mode (mi.in.get("FLAG")!="1")
 *  If the search is successful, the API return one line
 *  If the search is unsuccessful, the API send an error message
 *
 *
 * Date         Changed By    Description
 * 20221115     FLEBARS       Creation EXT-CMD02
 */
public class GetSupplyPath extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany
  private LinkedList<Object> responses
  private String errorMessage = ""
  private boolean checkSIGMA6 = false


  //Rounding parameters
  private Map<String, String> dtaGlobalRoundingParameters
  private Map<String, String> dtaCustomer

  public GetSupplyPath(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  /**
   * ALGORITHM:
   * initialize variables
   *  Get Order Flow Type ==> method getOrderFlowType
   *  if order flow type = 20
   *    Find Item ==> method findItem
   *    if found then addResponse ==> method addResponse
   *  else
   *    Find Item for FLTP 10 ==> method findItem
   *    if found then addResponse ==> method addResponse
   *    if not found
   *      Find Item for FLTP 40 ==> method findItem
   *      if found then addResponse ==> method addResponse
   *    end if
   *    if not found
   *      Find Item for FLTP 30 ==> method findItem
   *      if found then addResponse ==> method addResponse
   *    end if
   *  end if else
   *
   *  if found
   *      check replacement item ==> method getReplacementItem
   *      if replacement item
   *        read data from MITMAS, CUGEX1, EXT010
   *        addResponse ==> method addResponse
   *      end if
   *  end if
   *
   *  write mi responses
   *
   *
   */
  public void main() {
    //INITIALIZE VARIABLES
    currentCompany = (int) program.getLDAZD().CONO
    responses = new LinkedList<Object>()

    //Get API INPUTS
    String cuno = (String) mi.in.get("CUNO")
    String ortp = (String) mi.in.get("ORTP")
    String modl = (String) mi.in.get("MODL")
    String popn = (String) mi.in.get("POPN")
    String whlo = (String) mi.in.get("WHLO")
    double orqa = (Double) mi.in.get("ORQA")
    int flag = (Integer) mi.in.get("FLAG")
    int flg1 = (Integer) mi.in.get("FLG1")
    String alun = mi.in.get("ALUN") == null ? "" : (String) mi.in.get("ALUN")

    //Load global rounding parameters
    dtaGlobalRoundingParameters = getGlobalRoundingParameters()

    //Load customer data & rounding parameters
    dtaCustomer = getCustomerData(cuno)
    if (dtaCustomer == null) {
      mi.error("Client ${cuno} n'existe pas")
      return
    }
    if (!"20".equals(dtaCustomer["STAT"])) {
      mi.error("Client ${cuno} n'est pas valide")
      return
    }

    String order_fltp = getOrderTypeFlow(ortp)
    if (order_fltp == null || order_fltp == "") {
      mi.error("Type d'ordre non trouvé")
      return
    }

    String fwhl = getWareHouse(cuno, ortp, modl)
    if (fwhl == null)
      fwhl = whlo
    if (fwhl == null || "" == fwhl) {
      mi.error("Dépôt  non trouvé dans le modèle d'approvisionnement M3 écran MMS059")
      return
    }

    //logger.debug("method:main order_fltp:${order_fltp} fwhl:${fwhl}")


    boolean found = false
    String itno = null

    //IF ORDER FLTP = 20
    if ("20".equals(order_fltp)) {
      Map<String, String> dtaFindItem = findItem(cuno, popn, order_fltp, alun)
      if (this.errorMessage != "") {
        mi.error(this.errorMessage)
        return
      }
      if (dtaFindItem != null) {
        found = true
        itno = dtaFindItem["ITNO"].toString()
        addResponse(1, order_fltp, itno, fwhl, 1, (String) dtaFindItem["SUNO"], orqa, "", "", (String) dtaFindItem["HIE2"], (String) dtaFindItem["ASGD"])
      }
    } else {
      //ELSE ORDER FLTP != 20
      //LOOP 3 TIMES FOR
      //  10 warehouse flow type
      //  40 direct supplier flow type
      //  30 warehouse supplier flow type
      String active_fltp = ""
      for (int step = 0; step < 3; step++) {
        if (!found) {
          switch (step) {
            case 0:
              active_fltp = "10"
              break
            case 1:
              active_fltp = "40"
              break
            case 2:
              active_fltp = "30"
              break
            default:
              break
          }

          Map<String, String> dtaFindItem = findItem(cuno, popn, active_fltp, alun)
          if (this.errorMessage != "") {
            mi.error(this.errorMessage)
            return
          }
          logger.debug("method:main:active_fltp=${active_fltp}, dtaFindItem=" + dtaFindItem)


          if (dtaFindItem != null) {
            itno = dtaFindItem["ITNO"]
            String hie2 = dtaFindItem["HIE2"]
            String sule = dtaFindItem["SULE"]
            String suld = dtaFindItem["SULD"]
            String rscl = dtaFindItem["RSCL"]
            String cmde = dtaFindItem["CMDE"]
            String fvdt = dtaFindItem["FVDT"]
            String lvdt = dtaFindItem["LVDT"]
            String asgd = dtaFindItem["ASGD"]
            String cofa = dtaFindItem["COFA"]
            String dmcf = dtaFindItem["DMCF"]
            String dccd = dtaFindItem["DCCD"]
            //logger.debug("COFA:${cofa} DMCF:${dmcf} DCCD:${dccd}")
            //Depending flow type
            //10 warehouse flow type
            if ("10".equals(active_fltp)) {
              def dtaMITBAL = getItemDataFromMITBAL(fwhl, itno)
              if (dtaMITBAL != null) {
                String cpcd = dtaMITBAL["CPCD"] as String
                if (cpcd == "100") {
                  found = true
                  addResponse(1, active_fltp, itno, fwhl, 0, "", orqa, "", "", hie2, asgd)
                } else {
                  addResponse(0, active_fltp, itno, fwhl, 0, "", orqa, "cpcd non correspondant", "", hie2, asgd)
                }
              } else {
                addResponse(0, active_fltp, itno, fwhl, 0, "", orqa, "n'existe pas dans le dépôt", "", hie2, asgd)
              }
            } else if ("40".equals(active_fltp)) {
              //40 direct supplier flow type
              def dtaMITVEN = getItemDataFromMITVEN(suld, itno)
              if (dtaMITVEN != null) {
                String stat = dtaMITVEN["ISRS"].toString()
                String rem1 = ""
                double rounded_orqa = orqa
                double loqt = Double.parseDouble(dtaMITVEN["LOQT"].toString())
                def dtaRoundQty = roundQty(itno, hie2, orqa, flg1, cofa, dmcf, dccd)
                if (dtaRoundQty != null) {
                  rounded_orqa = Double.parseDouble((String) dtaRoundQty["ORQA"])
                  rem1 = (String) dtaRoundQty["REMK"]
                }
                logger.debug("orqa:${orqa} loqt:${loqt}")
                if ("20".equals(stat) && orqa >= loqt) {
                  found = true
                  addResponse(1, active_fltp, itno, fwhl, 1, suld, rounded_orqa, "", rem1, hie2, asgd)
                } else if (!"20".equals(stat)) {
                  addResponse(0, active_fltp, itno, fwhl, 1, suld, rounded_orqa, "article fournisseur non actif", rem1, hie2, asgd)
                } else if (orqa < loqt) {
                  addResponse(0, active_fltp, itno, fwhl, 1, suld, rounded_orqa, "qté commandé ${orqa} dépasse mini commande ${loqt}", rem1, hie2, asgd)
                }
              } else {
                addResponse(0, active_fltp, itno, fwhl, 1, suld, orqa, "article fournisseur non trouvé", "", hie2, asgd)
              }
            } else if ("30".equals(active_fltp)) {
              //30 warehouse supplier flow type
              found = true
              addResponse(1, active_fltp, itno, fwhl, 1, sule, orqa, "", "", hie2, asgd)
            }
          }
        }
      }
    }
    //logger.debug("method:main found=" + found)

    //CHECK REPLACEMENT ITEM
    if (found) {
      def dtaRITN = null
      if ("1".equals(dtaCustomer["CHB8"]))
        dtaRITN = getReplacementItem(cuno, itno, orqa, ortp, whlo, modl)
      if (dtaRITN != null) {
        String r_itno = dtaRITN["RITN"]
        String r_hie2 = dtaRITN["HIE2"]
        def dtaCUGEX1 = getItemDataFromCUGEX1MITMAS(itno)
        String r_fltp = dtaCUGEX1["FLTP"]
        String r_whlo = fwhl
        int r_ltyp = 0
        String r_suno = ""
        String r_asgd = ""
        double r_orqa = orqa
        String r_remk = ""
        if ("10".equals(r_fltp)) {
          r_ltyp = 0
          r_suno = ""
          r_remk = "remplace article:${itno}"
        } else if ("20".equals(r_fltp)) {
          r_ltyp = 1
          r_suno = dtaRITN["SUNO"]
          r_remk = "remplace article:${itno}"
        } else if ("30".equals(r_fltp)) {
          def dtaEXT010 = getItemDataFromEXT010(cuno, itno)
          if (dtaEXT010 != null) {
            r_suno = dtaEXT010["SULE"]
            r_asgd = dtaEXT010["ASGD"]
          }
          r_ltyp = 1
          r_remk = "remplace article:${itno}"
        } else if ("40".equals(r_fltp)) {
          def dtaEXT010 = getItemDataFromEXT010(cuno, itno)
          if (dtaEXT010 != null) {
            r_suno = dtaEXT010["SULD"]
          }
          r_ltyp = 1
          r_remk = "remplace article:${itno}"
        }

        //Update flag on last response
        int last_index = responses.size() - 1
        def lastResponse = responses.get(last_index)
        lastResponse["FLAG"] = 0
        lastResponse["REMK"] = "Remplacé par ${r_itno}"
        responses.set(last_index, lastResponse)


        //Round
        def dtaRoundQty = roundQty(r_itno, r_hie2, orqa, flg1, "1", "1", "0")
        double rounded_orqa = orqa
        String rnd_remk = ""

        if (dtaRoundQty != null) {
          rounded_orqa = Double.parseDouble((String) dtaRoundQty["ORQA"])
          rnd_remk = (String) dtaRoundQty["REMK"]
        }
        addResponse(1, r_fltp, r_itno, r_whlo, r_ltyp, r_suno, rounded_orqa, r_remk, rnd_remk, r_hie2, r_asgd)
      } else {
        //Round Qty for last response
        int last_index = responses.size() - 1
        def lastResponse = responses.get(last_index)
        if ("1".equals((String) lastResponse["FLAG"]) && !"40".equals((String) lastResponse["FLTP"])) {
          double t_orqa = Double.parseDouble((String) lastResponse["ORQA"])
          String t_itno = (String) lastResponse["ITNO"]
          String t_hie2 = (String) lastResponse["HIE2"]
          def dtaRoundQty = roundQty(t_itno, t_hie2, t_orqa, flg1, "1", "1", "1")
          lastResponse["ORQA"] = (String) dtaRoundQty["ORQA"]
          lastResponse["REMK"] = (String) lastResponse["REMK"] + " " + (String) dtaRoundQty["REMK"]
          responses.set(last_index, lastResponse)
        }
      }
    }


    //API Response
    // input flag = 1 and responses
    //send responses
    if (flag == 1 && responses.size() > 0) {
      for (def response in responses) {
        mi.outData.put("FLAG", (String) response["FLAG"])
        mi.outData.put("FLTP", (String) response["FLTP"])
        mi.outData.put("ITNO", (String) response["ITNO"])
        mi.outData.put("LTYP", (String) response["LTYP"])
        mi.outData.put("SUNO", (String) response["SUNO"])
        mi.outData.put("WHLO", (String) response["WHLO"])
        mi.outData.put("ORQA", (String) response["ORQA"])
        mi.outData.put("REMK", (String) response["REMK"])
        mi.outData.put("REM1", (String) response["REM1"])
        mi.outData.put("ASGD", (String) response["ASGD"])
        mi.write()
      }
    } else if (responses.size() > 0) {
      // input flag = 0 and responses
      //send last response
      int last_index = responses.size() - 1
      def response = responses.get(last_index)
      mi.outData.put("FLAG", (String) response["FLAG"])
      mi.outData.put("FLTP", (String) response["FLTP"])
      mi.outData.put("ITNO", (String) response["ITNO"])
      mi.outData.put("LTYP", (String) response["LTYP"])
      mi.outData.put("SUNO", (String) response["SUNO"])
      mi.outData.put("WHLO", (String) response["WHLO"])
      mi.outData.put("ORQA", (String) response["ORQA"])
      mi.outData.put("REMK", (String) response["REMK"])
      mi.outData.put("REM1", (String) response["REM1"])
      mi.outData.put("ASGD", (String) response["ASGD"])
      mi.write()
    } else {
      // no reponses
      // send error
      mi.error("Article non trouvé par le chemin d'approvisionnement")
    }
  }


  /**
   * Find Item by customer, popn, flow type
   *
   * ALGORITHM:
   * initialize variables
   * Loop on MITVEN20 with CONO, ALWT=1, ALWQ='', POPN=SIGMA6
   *   if not found
   *      read CUGEX1.MITMAS ==> method getItemDataFromCUGEX1MITMAS
   *      if CUGEX1.MITMAS.FLTP = fltp
   *        read EXT010 ==> method getItemDataFromEXT010
   *        if exists in EXT010
   *          populate responseObject
   *          found = true
   *        end if
   *      end if
   *   end if
   * End Loop
   *
   * if found
   *   return reponseObject
   * else
   *   return null
   * end if else
   *
   * @param cuno Customer
   * @param popn SIGMA6
   * @param fltp flow type
   * @return null if not found else structured object with ITNO, SULE, SULD, RSCL, CMDE, FVDT, LVDT
   */
  private Map<String, String> findItem(String cuno, String popn, String fltp, String alun) {
    //Method variable
    boolean found = false

    //Define return object structure
    def responseObject = [
      "ITNO": "",
      "HIE2": "",
      "SUNO": "",
      "SULE": "",
      "SULD": "",
      "RSCL": "",
      "CMDE": "",
      "FVDT": "",
      "LVDT": "",
      "ASGD": "",
      "COFA": "",
      "DMCF": "",
      "DCCD": ""
    ]

    //Database access
    DBAction queryMITPOP00 = database.table("MITPOP").index("20").selection(
      "MPCONO",
      "MPALWT",
      "MPALWQ",
      "MPPOPN",
      "MPE0PA",
      "MPVFDT",
      "MPITNO",
      "MPSEA1"
    ).build()

    //init query on MITPOP
    DBContainer containerMITPOP = queryMITPOP00.getContainer()
    containerMITPOP.set("MPCONO", currentCompany)
    containerMITPOP.set("MPALWT", 1)
    containerMITPOP.set("MPALWQ", "")
    containerMITPOP.set("MPPOPN", popn)

    //loop on MITPOP00 records
    Closure<?> readMITPOP = { DBContainer resultMITPOP ->
      if (!found) {
        String itno = resultMITPOP.getString("MPITNO").trim()
        def dataCUGEX1 = getItemDataFromCUGEX1MITMAS(itno)
        if (dataCUGEX1 != null) {
          String item_fltp = dataCUGEX1["FLTP"].toString()
          if (item_fltp.equals(fltp)) {
            def objEXT010 = getItemDataFromEXT010(cuno, itno)
            //logger.debug("findItem:cuno=${cuno},itno=${itno},objEXT010=" + objEXT010)
            if (objEXT010 != null) {
              Map<String, String> dtaMITMAS = getItemDataFromMITMAS(itno)
              Map<String, String> dtaMITAUN = getItemDataFromMITAUN(itno, alun)
              responseObject["ITNO"] = itno
              responseObject["HIE2"] = dtaMITMAS["HIE2"].toString()
              responseObject["SUNO"] = dtaMITMAS["SUNO"].toString()
              responseObject["SULE"] = objEXT010["SULE"].toString()
              responseObject["SULD"] = objEXT010["SULD"].toString()
              responseObject["RSCL"] = objEXT010["RSCL"].toString()
              responseObject["CMDE"] = objEXT010["CMDE"].toString()
              responseObject["FVDT"] = objEXT010["FVDT"].toString()
              responseObject["LVDT"] = objEXT010["LVDT"].toString()
              responseObject["ASGD"] = objEXT010["ASGD"].toString()
              responseObject["COFA"] = dtaMITAUN["COFA"].toString()
              responseObject["DMCF"] = dtaMITAUN["DMCF"].toString()
              responseObject["DCCD"] = dtaMITAUN["DCCD"].toString()
              found = true
            }
          }
        }
      }
    }
    if (queryMITPOP00.readAll(containerMITPOP, 4, readMITPOP) <= 0) {
      this.errorMessage = "SIGMA6 ${popn} inexistant"
    }
    if (!found && !this.checkSIGMA6) {
      if (!checkSIGMA6(cuno, popn)) {
        this.errorMessage = "SIGMA6 ${popn} non trouvé dans les assortiments client ${cuno}"
      }
    }
    this.checkSIGMA6 = true


    if (found)
      return responseObject
    else
      return null
  }

  /**
   * Check if popn exists in EXT010 for customer
   * @param cuno Customer
   * @param popn SIGMA6
   * @return null if not exists else structure with SULE, SULD, FVDT, LVDT
   */
  private boolean checkSIGMA6(String cuno, String popn) {
    //Define database access
    boolean found = false
    DBAction queryEXT01010 = database.table("EXT010")
      .index("10")
      .selection(
        "EXCONO",
        "EXCUNO",
        "EXSIG6",
        "EXASGD",
        "EXCDAT",
        "EXSIG6",
        "EXSULE",
        "EXSULD",
        "EXRSCL",
        "EXCMDE",
        "EXFVDT",
        "EXLVDT"
      ).build()


    //Query DB
    DBContainer containerEXT010 = queryEXT01010.getContainer()
    containerEXT010.set("EXCONO", currentCompany)
    containerEXT010.set("EXCUNO", cuno)
    containerEXT010.set("EXSIG6", popn)

    Closure<?> readEXT010 = { DBContainer resultEXT010 ->
      if (!found) {
        found = true
      }
    }

    queryEXT01010.readAll(containerEXT010, 3, readEXT010)

    return found
  }


  /**
   * Get Item Data from MITMAS
   * @param itno
   */
  private Map<String, String> getItemDataFromMITMAS(String itno) {
    //Define return object structure
    Map<String, String> responseObject = [
      "SUNO": "",
      "HIE2": "",
    ]

    DBAction queryMITMAS00 = database.table("MITMAS").index("00").selection(
      "MMCONO",
      "MMITNO",
      "MMSUNO",
      "MMHIE2"
    ).build()

    DBContainer containerMITMAS = queryMITMAS00.getContainer()
    containerMITMAS.set("MMCONO", currentCompany)
    containerMITMAS.set("MMITNO", itno)
    if (queryMITMAS00.read(containerMITMAS)) {
      responseObject["SUNO"] = containerMITMAS.getString("MMSUNO").trim()
      responseObject["HIE2"] = containerMITMAS.getString("MMHIE2").trim()
      return responseObject
    }
    return null
  }

  /**
   * Get Item Data from MITAUN
   * @param itno
   * @param alun
   */
  private Map<String, String> getItemDataFromMITAUN(String itno, String alun) {
    Map<String, String> responseObject = [
      "COFA": "1"
      , "DMCF": "1"
      , "DCCD": ""
    ]

    if (alun.length() == 0)
      return responseObject

    DBAction mitaunQuery = database.table("MITAUN").index("00").selection(
      "MUCONO",
      "MUITNO",
      "MUAUTP",
      "MUALUN",
      "MUCOFA",
      "MUDMCF",
      "MUDCCD"
    ).build()

    DBContainer mitaunRequest = mitaunQuery.getContainer()
    mitaunRequest.set("MUCONO", currentCompany)
    mitaunRequest.set("MUITNO", itno)
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", alun)
    if (mitaunQuery.read(mitaunRequest)) {
      //logger.debug("found MITAUN")
      responseObject["COFA"] = mitaunRequest.get("MUCOFA") as String
      responseObject["DMCF"] = mitaunRequest.get("MUDMCF") as String
      responseObject["DCCD"] = mitaunRequest.get("MUDCCD") as String
      return responseObject
    }
    return null
  }

  /**
   * Get Item Data from MITBAL
   * @param whlo
   * @param itno
   */
  private Map<String, String> getItemDataFromMITBAL(String whlo, String itno) {
    //Define return object structure
    Map<String, String> responseObject = [
      "IDDT": "",
      "CPCD": "",
      "STAT": ""
    ]

    DBAction queryMITBAL00 = database.table("MITBAL").index("00").selection(
      "MBCONO",
      "MBWHLO",
      "MBITNO",
      "MBSTAT",
      "MBCPCD",
      "MBIDDT"
    ).build()

    DBContainer containerMITBAL = queryMITBAL00.getContainer()
    containerMITBAL.set("MBCONO", currentCompany)
    containerMITBAL.set("MBWHLO", whlo)
    containerMITBAL.set("MBITNO", itno)
    if (queryMITBAL00.read(containerMITBAL)) {
      responseObject["STAT"] = ((String) containerMITBAL.get("MBSTAT")).trim()
      responseObject["IDDT"] = ((String) containerMITBAL.get("MBIDDT")).trim()
      responseObject["CPCD"] = ((String) containerMITBAL.get("MBCPCD")).trim()
      return responseObject
    }
    return null
  }

  /**
   * Get Item Data from MITVEN
   * @param suno
   * @param itno
   */
  private Map<String, String> getItemDataFromMITVEN(String suno, String itno) {
    //Define return object structure
    Map<String, String> responseObject = [
      "ISRS": "",
      "LOQT": ""
    ]

    DBAction queryMITVEN00 = database.table("MITVEN").index("00").selection(
      "IFCONO",
      "IFITNO",
      "IFPRCS",
      "IFSUFI",
      "IFSUNO",
      "IFISRS",
      "IFLOQT"
    ).build()

    DBContainer containerMITVEN = queryMITVEN00.getContainer()
    containerMITVEN.set("IFCONO", currentCompany)
    containerMITVEN.set("IFITNO", itno)
    containerMITVEN.set("IFSUNO", suno)
    if (queryMITVEN00.read(containerMITVEN)) {
      responseObject["ISRS"] = containerMITVEN.get("IFISRS").toString()
      responseObject["LOQT"] = containerMITVEN.get("IFLOQT").toString()
      //logger.debug("getItemDataFromMITVEN itno:${itno} suno:${suno} response:${responseObject}")
      return responseObject
    }
    return null
  }

  /**
   * Check if item exists in EXT010 for customer
   * @param cuno Customer
   * @param itno Item
   * @return null if not exists else structure with SULE, SULD, FVDT, LVDT
   */
  private Map<String, String> getItemDataFromEXT010(String cuno, String itno) {
    boolean found = false
    //Define return object structure
    Map<String, String> responseObject = [
      "SULE": "",
      "SULD": "",
      "RSCL": "",
      "CMDE": "",
      "FVDT": "",
      "LVDT": "",
      "ASGD": ""
    ]

    //Define database access
    DBAction queryEXT01002 = database.table("EXT010").index("02").selection(
      "EXCONO",
      "EXCUNO",
      "EXITNO",
      "EXASGD",
      "EXCDAT",
      "EXSIG6",
      "EXSULE",
      "EXSULD",
      "EXRSCL",
      "EXCMDE",
      "EXFVDT",
      "EXLVDT"
    ).build()


    //Query DB
    DBContainer containerEXT010 = queryEXT01002.getContainer()
    containerEXT010.set("EXCONO", currentCompany)
    containerEXT010.set("EXCUNO", cuno)
    containerEXT010.set("EXITNO", itno)

    Closure<?> readEXT010 = { DBContainer resultEXT010 ->
      if (!found) {
        responseObject["ASGD"] = ((String) resultEXT010.get("EXASGD")).trim()
        responseObject["SULE"] = ((String) resultEXT010.get("EXSULE")).trim()
        responseObject["SULE"] = ((String) resultEXT010.get("EXSULE")).trim()
        responseObject["SULD"] = ((String) resultEXT010.get("EXSULD")).trim()
        responseObject["RSCL"] = ((String) resultEXT010.get("EXRSCL")).trim()
        responseObject["CMDE"] = ((String) resultEXT010.get("EXCMDE")).trim()
        responseObject["FVDT"] = ((String) resultEXT010.get("EXFVDT")).trim()
        responseObject["LVDT"] = ((String) resultEXT010.get("EXLVDT")).trim()
        found = true
      }
    }

    queryEXT01002.readAll(containerEXT010, 3, readEXT010)

    if (found)
      return responseObject
    else
      return null
  }

  /**
   * Get Item Data from CUGEX1.MITMAS
   * @param itno
   */
  private Map<String, String> getItemDataFromCUGEX1MITMAS(String itno) {
    //Define return object structure
    Map<String, String> responseObject = [
      "FLTP": ""
    ]

    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection("F1CONO",
      "F1FILE",
      "F1PK01",
      "F1PK02",
      "F1PK03",
      "F1PK04",
      "F1PK05",
      "F1PK06",
      "F1PK07",
      "F1PK08",
      "F1A830"
    ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "MITMAS")
    containerCUGEX1.set("F1PK01", itno)

    if (queryCUGEX100.read(containerCUGEX1)) {
      responseObject["FLTP"] = containerCUGEX1.get("F1A830").toString().trim()
      return responseObject
    }
    return null
  }


  /**
   * getOrderTypeFlow
   * Get order type with CUSEXTMI
   * @param orty Order Type
   * @return order type flow cugex1.ootype.A830
   */
  private String getOrderTypeFlow(String orty) {

    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection("F1CONO",
      "F1FILE",
      "F1PK01",
      "F1PK02",
      "F1PK03",
      "F1PK04",
      "F1PK05",
      "F1PK06",
      "F1PK07",
      "F1PK08",
      "F1A030"
    ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "OOTYPE")
    containerCUGEX1.set("F1PK01", orty)

    if (queryCUGEX100.read(containerCUGEX1)) {
      return containerCUGEX1.get("F1A030").toString().trim()
    }
  }


  /**
   * Get replacement item thru api OIS340MI/LstSupplSummary
   * @param suno
   * @param itno
   */
  private Map<String, String> getReplacementItem(String cuno, String itno, double orqa, String ortp, String whlo, String modl) {
    String ritn = null
    boolean found = false

    //Define return object structure
    Map<String, String> responseObject = [
      "RITN": "",
      "HIE2": "",
      "SUNO": ""
    ]


    def params = [
      "CUNO": cuno,
      "FACI": "E10",
      "POPN": itno,
      "ORQA": "" + orqa,
      "ORTP": ortp,
      "SPLM": "CSN00"
    ]
    def callback = { Map<String, String> response ->
      //logger.debug("OIS340MI response " + response)
      if (response.ITNO != null && !itno.equals(response.ITNO) && !found) {
        ritn = response.ITNO.toString()
        found = true
      }
    }
    miCaller.call("OIS340MI", "LstSupplSummary", params, callback)

    if (found) {
      //logger.debug("OIS340MI ritn " + ritn)
      def dtaMITMAS = getItemDataFromMITMAS(ritn)
      if (dtaMITMAS != null) {
        //logger.debug("OIS340MI ritn2 " + ritn)
        responseObject["RITN"] = ritn
        responseObject["SUNO"] = (String) dtaMITMAS["SUNO"]
        responseObject["HIE2"] = (String) dtaMITMAS["HIE2"]
        return responseObject
      }
    }

    return null
  }


  /**
   * Add response into responses list
   */
  private void addResponse(int flag, String fltp, String itno, String whlo, int ltyp, String suno, double orqa, String remk, String rem1, String hie2, String asgd) {
    def responseData = [
      "FLAG": "" + flag,
      "FLTP": "" + fltp,
      "ITNO": "" + itno,
      "WHLO": "" + whlo,
      "LTYP": "" + ltyp,
      "SUNO": "" + suno,
      "ORQA": "" + orqa,
      "REMK": "" + remk,
      "REM1": "" + rem1,
      "HIE2": "" + hie2,
      "ASGD": "" + asgd
    ]
    //logger.debug("response:" + responseData.toString())
    responses.add(responseData)
  }

  /**
   * Get warehouse thru API MMS059MI
   */
  private String getWareHouse(String cuno, String ortp, String modl) {
    def params = [
      "SPLM": "CSN01",
      "PREX": "5",
      "SPLA": "10",
      "OBV1": cuno,
      "OBV2": ortp,
      "OBV3": modl,
    ]
    String fwhl = null
    def callback = { Map<String, String> response ->
      //logger.debug(response.toString())
      if (response.FWHL != null) {
        fwhl = response.FWHL.toString()
      }
    }
    //1ST CALL WITH MODL
    miCaller.call("MMS059MI", "Get", params, callback)

    //2ND CALL WITHOUT MODL
    if (fwhl == null) {
      params = [
        "SPLM": "CSN01",
        "PREX": "5",
        "SPLA": "10",
        "OBV1": cuno,
        "OBV2": ortp
      ]
      miCaller.call("MMS059MI", "Get", params, callback)
    }

    return fwhl
  }

  /**
   * Get Customer Informations from OCUSMA and CUGEX1
   * @param cuno Customer
   * @return null if not found else structure with STAT, CHB7
   */
  private Map<String, String> getCustomerData(String cuno) {
    Map<String, String> responseObject = [
      "STAT": "",
      "CHB7": "",
      "CHB8": ""
    ]

    DBAction queryOCUSMA00 = database.table("OCUSMA").index("00").selection(
      "OKCONO",
      "OKCUNO",
      "OKSTAT"
    ).build()

    DBContainer containerOCUSMA = queryOCUSMA00.getContainer()
    containerOCUSMA.set("OKCONO", currentCompany)
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
      "F1CHB7",
      "F1CHB8"
    ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "OCUSMA")
    containerCUGEX1.set("F1PK01", cuno)

    if (queryCUGEX100.read(containerCUGEX1)) {
      responseObject["CHB7"] = containerCUGEX1.get("F1CHB7").toString()
      responseObject["CHB8"] = containerCUGEX1.get("F1CHB8").toString()
    }
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
  private Map<String, String> getGlobalRoundingParameters() {
    //Define return object structure
    Map<String, String> responseObject = [
      "UPA_SUP": "",
      "UPA_INF": "",
      "UDP_SUP": "",
      "UDP_INF": "",
      "UCO_SUP": "",
      "UCO_INF": ""
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
      responseObject["UPA_SUP"] = containerCUGEX1.get("F1N296").toString()
      responseObject["UPA_INF"] = containerCUGEX1.get("F1N596").toString()
      responseObject["UDP_SUP"] = containerCUGEX1.get("F1N396").toString()
      responseObject["UDP_INF"] = containerCUGEX1.get("F1N696").toString()
      responseObject["UCO_SUP"] = containerCUGEX1.get("F1N496").toString()
      responseObject["UCO_INF"] = containerCUGEX1.get("F1N796").toString()
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
  private Map<String, String> getItemRoundingParameter(String itno, String hie2) {
    //Define return object structure
    Map<String, String> responseObject = [
      "UPA_SUP": "",
      "UPA_INF": "",
      "UDP_SUP": "",
      "UDP_INF": "",
      "UCO_SUP": "",
      "UCO_INF": "",
      "COF_UPA": "",
      "COF_UDP": "",
      "COF_UCO": ""
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
    containerCUGEX1.set("F1FILE", "MITHRY")
    containerCUGEX1.set("F1PK01", "2")
    containerCUGEX1.set("F1PK02", hie2)

    if (queryCUGEX100.read(containerCUGEX1)) {
      responseObject["UPA_SUP"] = containerCUGEX1.get("F1N296").toString()
      responseObject["UPA_INF"] = containerCUGEX1.get("F1N596").toString()
      responseObject["UDP_SUP"] = containerCUGEX1.get("F1N396").toString()
      responseObject["UDP_INF"] = containerCUGEX1.get("F1N696").toString()
      responseObject["UCO_SUP"] = containerCUGEX1.get("F1N496").toString()
      responseObject["UCO_INF"] = containerCUGEX1.get("F1N796").toString()
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
      cofa = new BigDecimal(Double.toString(cofa)).setScale(6, RoundingMode.HALF_UP).doubleValue()
      if ("UPA".equals(alun)) {
        responseObject["COF_UPA"] = "" + cofa
      } else if ("UDP".equals(alun)) {
        responseObject["COF_UDP"] = "" + cofa
      } else if ("UCO".equals(alun)) {
        responseObject["COF_UCO"] = "" + cofa
      }
    }
    queryMITAUN00.readAll(containerMITAUN, 3, readMITAUN)
    //logger.debug(responseObject.toString())

    return responseObject
  }

  /**
   *
   * @param itno
   * @param hie2
   * @param orqa
   * @param flg1
   * @param cofa
   * @param dmcf
   * @param dccd
   * @return
   */
  private Map<String, String> roundQty(String itno, String hie2, double orqa, int flg1, String icofa, String idmcf, String idccd) {
    //logger.debug("method:roundQty ")
    //Define return object structure
    Map<String, String> responseObject = [
      "ORQA": "",
      "REMK": ""
    ]

    //unit conversion for qty
    if (!"1".equals(icofa)){
      double dCofa = icofa as double
      int dDccd = idccd as int
      orqa = "1".equals(idmcf) ? orqa * dCofa : orqa / dCofa
      orqa = new BigDecimal(Double.toString(orqa)).setScale(dDccd, RoundingMode.HALF_UP).doubleValue()
    }


    if (flg1 == 0) {
      responseObject["ORQA"] = "" + orqa
      responseObject["REMK"] = "Pas d'arrondi"
      return responseObject
    }

    //PAS D'ARRONDI CLIENT
    if (!dtaCustomer["CHB7"].equals("1")) {
      responseObject["ORQA"] = "" + orqa
      responseObject["REMK"] = "Pas d'arrondi"
      return responseObject
    }

    //Read Item data
    def dtaItemRoudingParameters = getItemRoundingParameter(itno, hie2)

    boolean rounded = false
    boolean roundingRuleFound = false
    double outqty = 0d
    String remk = ""

    double gen_upa_sup = 0
    double gen_upa_inf = 0
    double gen_udp_sup = 0
    double gen_udp_inf = 0
    double gen_uco_sup = 0
    double gen_uco_inf = 0

    double rayon_upa_sup = 0
    double rayon_upa_inf = 0
    double rayon_udp_sup = 0
    double rayon_udp_inf = 0
    double rayon_uco_sup = 0
    double rayon_uco_inf = 0

    double cof_upa = 0
    double cof_udp = 0
    double cof_uco = 0


    try {
      gen_upa_sup = Double.parseDouble(dtaGlobalRoundingParameters["UPA_SUP"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      gen_upa_inf = Double.parseDouble(dtaGlobalRoundingParameters["UPA_INF"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      gen_udp_sup = Double.parseDouble(dtaGlobalRoundingParameters["UDP_SUP"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      gen_udp_inf = Double.parseDouble(dtaGlobalRoundingParameters["UDP_INF"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      gen_uco_sup = Double.parseDouble(dtaGlobalRoundingParameters["UCO_SUP"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      gen_uco_inf = Double.parseDouble(dtaGlobalRoundingParameters["UCO_INF"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      rayon_upa_sup = Double.parseDouble(dtaItemRoudingParameters["UPA_SUP"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      rayon_upa_inf = Double.parseDouble(dtaItemRoudingParameters["UPA_INF"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      rayon_udp_sup = Double.parseDouble(dtaItemRoudingParameters["UDP_SUP"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      rayon_udp_inf = Double.parseDouble(dtaItemRoudingParameters["UDP_INF"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      rayon_uco_sup = Double.parseDouble(dtaItemRoudingParameters["UCO_SUP"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      rayon_uco_inf = Double.parseDouble(dtaItemRoudingParameters["UCO_INF"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      cof_upa = Double.parseDouble(dtaItemRoudingParameters["COF_UPA"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      cof_udp = Double.parseDouble(dtaItemRoudingParameters["COF_UDP"].toString())
    } catch (NumberFormatException e) {
    }
    try {
      cof_uco = Double.parseDouble(dtaItemRoudingParameters["COF_UCO"].toString())
    } catch (NumberFormatException e) {
    }


    //
    for (int step = 0; step < 6; step++) {
      if (!roundingRuleFound) {
        double lim_sup = 0
        double lim_inf = 0
        double cofa = 0
        switch (step) {
          case 0:
            lim_sup = rayon_upa_sup
            lim_inf = rayon_upa_inf
            cofa = cof_upa
            remk = "rayon palette "
            break
          case 1:
            lim_sup = rayon_udp_sup
            lim_inf = rayon_udp_inf
            cofa = cof_udp
            remk = "rayon 1/2 palette "
            break
          case 2:
            lim_sup = rayon_uco_sup
            lim_inf = rayon_uco_inf
            cofa = cof_uco
            remk = "rayon couche "
            break
          case 3:
            lim_sup = gen_upa_sup
            lim_inf = gen_upa_inf
            cofa = cof_upa
            remk = "global palette "
            break
          case 4:
            lim_sup = gen_udp_sup
            lim_inf = gen_udp_inf
            cofa = cof_udp
            remk = "global 1/2 palette "
            break
          case 5:
            lim_sup = gen_uco_sup
            lim_inf = gen_uco_inf
            cofa = cof_uco
            remk = "global couche "
            break
          default:
            break
        }
        //logger.debug("step ${step} lim_sup ${lim_sup} lim_inf ${lim_inf} cofa ${cofa}")

        //Calcul
        if ((lim_sup > 0 || lim_inf > 0) && cofa > 0) {

          //logger.debug("step ${step} lim_sup1 ${lim_sup} lim_inf1 ${lim_inf} cofa1 ${cofa}")
          if (step == 2 || step == 5) { //Si on est sur step 2 ou 5 on sort même si la regle ne s'applique pas
            roundingRuleFound = true
          }

          int nb_un = (int) (orqa / cofa)//Nb unit
          double reste = new BigDecimal(Double.toString(orqa - (nb_un * cofa))).setScale(6, RoundingMode.HALF_UP).doubleValue()
          double preste = new BigDecimal(Double.toString(reste / cofa)).setScale(6, RoundingMode.HALF_UP).doubleValue()
          double lim_qty_sup = new BigDecimal(Double.toString(cofa * lim_sup / 100)).setScale(6, RoundingMode.HALF_UP).doubleValue()
          double lim_qty_inf = new BigDecimal(Double.toString(cofa * lim_inf / 100)).setScale(6, RoundingMode.HALF_UP).doubleValue()
          //logger.debug("step ${step} orqa ${orqa} cofa ${cofa} nb un ${nb_un}  reste ${reste} % ${preste} lim_qty_sup ${lim_qty_sup} lim_qty_inf ${lim_qty_inf}"  )
          //logger.debug("step=${step},remk=${remk},reste=${reste},%reste=${preste},lim_sup=${lim_sup},lim_inf=${lim_inf}")

          if (preste >= lim_sup && reste != 0) { //Cas sup
            //logger.debug("round > " + (preste >= lim_sup && reste != 0))
            rounded = true
            if (step == 0 || step == 3) {//Si on est sur step 0 ou 3 on sort seulementsi la régle s'applique
              roundingRuleFound = true
            }
            outqty = new BigDecimal(Double.toString((nb_un + 1) * cofa)).setScale(6, RoundingMode.HALF_UP).doubleValue()
            remk += " > " + (lim_sup * 100) + "%"
          } else if (preste < lim_inf && reste != 0) { //Cas inf
            if (step == 0 || step == 3) {//Si on est sur step 0 ou 3 on sort seulementsi la régle s'applique
              roundingRuleFound = true
            }
            rounded = true
            outqty = new BigDecimal(Double.toString((nb_un) * cofa)).setScale(6, RoundingMode.HALF_UP).doubleValue()
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
      responseObject["ORQA"] = "" + orqa
      responseObject["REMK"] = "Pas d'arrondi"
      if (roundingRuleFound) {
        responseObject["REMK"] = "Pas d'arrondi mais une regle a été trouvé"
      }
      return responseObject
    } else {
      if (outqty == 0)
        outqty = 1
      responseObject["ORQA"] = "" + outqty
      responseObject["REMK"] = remk
      return responseObject
    }
  }
}
