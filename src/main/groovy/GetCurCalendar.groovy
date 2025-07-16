/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT042MI.GetCurCalendar
 * Description : The GetCurCalendar transaction retrieve actually calendar for one customer.
 * Date         Changed By   Description
 * 20240124     YVOYOU       COMX02 - Cadencier
 * 20250416     ARENARD      The code has been checked
 * 20250617     FLEBARS      Add comment on output CDNN for validation
 */
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter

public class GetCurCalendar extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany
  private String previousCalendar
  private Integer creationDate

  public GetCurCalendar(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    currentCompany

    // Check company
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO") as Integer
    }

    // Check customer
    if(mi.in.get("CUNO") != null){
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer OCUSMA = ocusmaQuery.getContainer()
      OCUSMA.set("OKCONO",currentCompany)
      OCUSMA.set("OKCUNO",mi.in.get("CUNO"))
      if (!ocusmaQuery.read(OCUSMA)) {
        mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
    }else{
      mi.error("Code Client est obligatoire")
      return
    }
    previousCalendar = ""
    logger.debug("CONO = " + currentCompany)
    logger.debug("CUNO = " + mi.in.get("CUNO"))
    DBAction query = database.table("EXT042").index("00").selection("EXCDNN", "EXRGDT").reverse().build()
    DBContainer EXT042 = query.getContainer()
    EXT042.set("EXCONO", currentCompany)
    EXT042.set("EXCUNO", mi.in.get("CUNO"))
    if(!query.readAll(EXT042, 2, 1, outDataExt042)){}

    if(previousCalendar.trim() == "") {
      mi.error("Pas de cadencier existant – Veuillez faire éditer")
      return
    }
    //Date controle
    LocalDate dateIlYAA7Jours = LocalDate.now().minusDays(7)

    // Formater la date au format AAAAMMJJ
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    String dateFormatee = dateIlYAA7Jours.format(formatter)
    if (creationDate<Integer.parseInt(dateFormatee)) {
      mi.error("Le précédent cadencier ne peut pas être réédité il a été créé y a plus de 7 jours – Veuillez faire éditer")
      return
    }
    mi.outData.put("CDNN", previousCalendar)
    mi.write()
  }

  /**
   * This closure is used to get the data from EXT042
   * @param EXT042
   */
  Closure<?> outDataExt042 = { DBContainer EXT042 ->
    logger.debug("found EXT042 : " + EXT042.get("EXCDNN"))
    previousCalendar = EXT042.get("EXCDNN")
    creationDate = EXT042.get("EXRGDT")
  }
}
