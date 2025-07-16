/****************************************************************************************
 Extension Name: EXT420MI.DltPickingList
 Type: ExtendM3Transaction
 Script Author: FLEBARS
 Date: 2025-02-02
 Description:
 * The DltPickingList transaction calls MWS420 option 4.

 Revision History:
 Name                    Date        Version    Description of Changes
 FLEBARS                 2025-02-02  1.0        LOG14 – Picking
 ARENARD                 2025-04-16  1.1        The code has been checked
 ARENARD                 2025-04-25  1.2        Program header formatted
 ******************************************************************************************/
public class DltPickingList extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany
  private String currentDivision
  private final IonAPI ion


  public DltPickingList(MIAPI mi, IonAPI ion, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.ion = ion
    this.utility = utility
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Check DLIX
    if (mi.in.get("DLIX") == null || mi.in.get("DLIX") == "") {
      mi.error("Numéro indexe est obligatoire")
      return
    }

    // Check PLSX
    if (mi.in.get("PLSX") == null || mi.in.get("PLSX") == "") {
      mi.error("Numéro suffixe est obligatoire")
      return
    }

    // Check WHLO
    if (mi.in.get("WHLO") == null || mi.in.get("WHLO") == "") {
      mi.error("Dépot est obligatoire")
      return
    }

    currentDivision = (String) program.getLDAZD().DIVI
    String iDLIX = mi.in.get("DLIX")
    String iPLSX = mi.in.get("PLSX")
    String iWHLO = mi.in.get("WHLO")

    String iOPT2 = " 4"
    String iSPIC = "A"

    logger.debug("iOPT2 = " + iOPT2)
    logger.debug("iPLSX = " + iPLSX)
    logger.debug("iDLIX = " + iDLIX)
    logger.debug("iWHLO = " + iWHLO)
    logger.debug("iSPIC = " + iSPIC)
    String endpoint = "/M3/ips/service/MWS420"
    Map<String, String> headers = ["Accept": "application/xml", "Content-Type": "application/xml"]
    Map<String, String> queryParameters = (Map) null
    Map<String, String> formParameters = (Map) null // post URL encoded parameters
    String body = ""

    body = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:cred=\"http://lawson.com/ws/credentials\" xmlns:dlt=\"http://schemas.infor.com/ips/MWS420Bis/DltPickingList\">" +
      "<soapenv:Header><cred:lws><cred:company>" + currentCompany + "</cred:company><cred:division>" + currentDivision + "</cred:division></cred:lws>" +
      "</soapenv:Header>" +
      "<soapenv:Body>" +
      "<dlt:DltPickingList><dlt:MWS420><dlt:DeliveryNumber>" + iDLIX.replace(".0", "") + "</dlt:DeliveryNumber><dlt:PickingListSuffix>" + iPLSX.replace(".0", "") + "</dlt:PickingListSuffix><dlt:Warehouse>" + iWHLO + "</dlt:Warehouse><dlt:OpeningPanel>"+ iSPIC + "</dlt:OpeningPanel><dlt:Option>" + iOPT2 + "</dlt:Option></dlt:MWS420></dlt:DltPickingList></soapenv:Body></soapenv:Envelope>"
    logger.debug("Step 2 endpoint = " + endpoint)
    logger.debug("Step 2 headers = " + headers)
    logger.debug("Step 2 queryParameters = " + queryParameters)
    logger.debug("Step 2 body = " + body)
    IonResponse response = ion.post(endpoint, headers, queryParameters, body)
    if (response.getError()) {
      logger.debug("Failed calling ION API, detailed error message: ${response.getErrorMessage()}")
      logger.debug("Failed calling ION API, detailed error message: ${body}")
      mi.error("Failed calling ION API, detailed error message: ${response.getErrorMessage()}")
      return
    }
    if (response.getStatusCode() != 200) {
      logger.debug("Expected status 200 but got ${response.getStatusCode()} instead ${response.getContent()} ")
      logger.debug("Failed calling ION API, detailed error message: ${body}")
      mi.error("Expected status 200 but got ${response.getStatusCode()} instead ${response.getContent()} ")
      return
    }
  }
}
