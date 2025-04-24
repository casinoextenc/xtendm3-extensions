/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT040MI.RtvNextCalendar
 * Description : The RtvNextCalendar transaction retrieve next calendar code for one customer.
 * Date         Changed By   Description
 * 20230317     ARENARD      COMX02 - Cadencier
 * 20250416     ARENARD      The code has been checked
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class RtvNextCalendar extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany
  private String previousCalendar
  private String currentCalendar
  private String currentCalendarYearWeek
  private String previousCalendarYearWeek
  private String previousCalendarSuffix
  private String previousCalendarNextSuffix

  public RtvNextCalendar(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program,UtilityAPI utility) {
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
      DBAction queryOcusma = database.table("OCUSMA").index("00").build()
      DBContainer OCUSMA = queryOcusma.getContainer()
      OCUSMA.set("OKCONO",currentCompany)
      OCUSMA.set("OKCUNO",mi.in.get("CUNO"))
      if (!queryOcusma.read(OCUSMA)) {
        mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
    }else{
      mi.error("Code Client est obligatoire")
      return
    }

    retrievePreviousCalendar()
    retrieveCurrentCalendar()

    mi.outData.put("CDNN", currentCalendar)
    mi.write()
  }
  /**
   * Retrieve previous calendar name
   * @return
   */
  private void retrievePreviousCalendar () {
    previousCalendar = ""
    previousCalendarYearWeek = ""
    previousCalendarSuffix = ""
    previousCalendarNextSuffix = ""
    logger.debug("currentCompany = " + currentCompany)
    DBAction query = database.table("EXT042").index("00").selection("EXCDNN").reverse().build()
    DBContainer EXT042 = query.getContainer()
    EXT042.set("EXCONO", currentCompany)
    EXT042.set("EXCUNO", mi.in.get("CUNO"))
    if(!query.readAll(EXT042, 2, 1, outDataExt042)){}
    if(previousCalendar.trim() != "") {
      previousCalendarYearWeek = previousCalendar.substring(0,6)

      int sfx = 0
      try {
        sfx = Integer.parseInt(previousCalendar.substring(6,9))
      } catch (NumberFormatException e) {
        sfx = 0
      }
      sfx += 1

      previousCalendarNextSuffix = String.format("%03d", sfx)
    }
    logger.debug("previousCalendar = " + previousCalendar)
    logger.debug("previousCalendarYearWeek = " + previousCalendarYearWeek)
    logger.debug("previousCalendarSuffix = " + previousCalendarSuffix)
    logger.debug("previousCalendarNextSuffix = " + previousCalendarNextSuffix)
  }
  /**
   * Closure to handle the output of EXT042
   * @param EXT041
   */
  Closure<?> outDataExt042 = { DBContainer EXT041 ->
    logger.debug("found EXT041")
    previousCalendar = EXT041.get("EXCDNN")
  }
  /**
   * Retrieve current calendar name
   * @return
   */
  private void retrieveCurrentCalendar() {
    currentCalendar = ""
    currentCalendarYearWeek = ""

    Calendar cal = Calendar.getInstance()
    Date date = cal.getTime()
    cal.setTime(date)
    int currentYear = cal.get(Calendar.YEAR)
    int currentWeek = cal.get(Calendar.WEEK_OF_YEAR)

    currentCalendarYearWeek = (currentYear as String) + String.format("%02d", currentWeek)
    logger.debug("currentCalendarYearWeek = " + currentCalendarYearWeek)

    if(currentCalendarYearWeek != previousCalendarYearWeek) {
      currentCalendar = currentCalendarYearWeek + "001"
    } else {
      currentCalendar = currentCalendarYearWeek + previousCalendarNextSuffix
    }
    logger.debug("currentCalendar = " + currentCalendar)
  }
}
