/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT021MI.LstAssortHist
 * Description : The LstAssortHist transaction list records to the EXT021 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 */
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
public class LstAssortHist extends ExtendM3Transaction {
	private final MIAPI mi;
	private final DatabaseAPI database
	private final LoggerAPI logger
	private final MICallerAPI miCaller;
	private final ProgramAPI program;
	private final UtilityAPI utility;

	public LstAssortHist(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
		this.mi = mi;
		this.database = database;
		this.logger = logger;
		this.program = program;
		this.utility = utility;
	}

	public void main() {
		Integer currentCompany;
		String cuno = "";
		String ascd = "";
		String fdat = "";
		String type = "";
		if (mi.in.get("CONO") == null) {
			currentCompany = (Integer) program.getLDAZD().CONO;
		} else {
			currentCompany = mi.in.get("CONO");
		}

		if (mi.in.get("CUNO") != null) {
			DBAction countryQuery = database.table("OCUSMA").index("00").build();
			DBContainer OCUSMA = countryQuery.getContainer();
			OCUSMA.set("OKCONO", currentCompany);
			OCUSMA.set("OKCUNO", mi.in.get("CUNO"));
			if (!countryQuery.read(OCUSMA)) {
				mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas");
				return;
			}
			cuno = mi.in.get("CUNO");
		}

		if (mi.in.get("ASCD") != null) {
			DBAction countryQuery = database.table("CSYTAB").index("00").build();
			DBContainer CSYTAB = countryQuery.getContainer();
			CSYTAB.set("CTCONO", currentCompany);
			CSYTAB.set("CTSTCO", "ASCD");
			CSYTAB.set("CTSTKY", mi.in.get("ASCD"));
			if (!countryQuery.read(CSYTAB)) {
				mi.error("Code Assortiment  " + mi.in.get("ASCD") + " n'existe pas");
				return;
			}
			ascd = mi.in.get("ASCD");
		}

		if (mi.in.get("FDAT") != null) {
			fdat = mi.in.get("FDAT");
			if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
				mi.error("Format Date de Validité incorrect");
				return;
			}
		}

		if (mi.in.get("TYPE") != null) {
			type = mi.in.get("TYPE");
		}

		//Create Expression
		ExpressionFactory expression = database.getExpressionFactory("EXT021");
		expression = expression.eq("EXCONO", currentCompany.toString());
		if (cuno != "") {
			expression = expression.and(expression.eq("EXCUNO", cuno));
		}
		if (ascd != "") {
			expression = expression.and(expression.eq("EXASCD", ascd));
		}
		if (fdat != "") {
			expression = expression.and(expression.ge("EXFDAT", fdat));
		}
		if (type != "") {
			expression = expression.and(expression.eq("EXTYPE", type));
		}
		//Run Select
		DBAction query = database.table("EXT021").index("00").matching(expression).selection("EXCONO", "EXASCD", "EXCUNO", "EXFDAT", "EXTYPE", "EXCHB1", "EXDATA", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID", "EXTX60").build();
		DBContainer EXT021 = query.getContainer();
		EXT021.setInt("EXCONO", currentCompany);
		if (!query.readAll(EXT021, 1, outData)) {
			mi.error("L'enregistrement n'existe pas");
			return;
		}
	}
	Closure<?> outData = {
		DBContainer EXT021 ->
		String cono = EXT021.get("EXCONO");
		String ascd = EXT021.get("EXASCD");
		String cuno = EXT021.get("EXCUNO");
		String fdat = EXT021.get("EXFDAT");
		String type = EXT021.get("EXTYPE");
		String chb1 = EXT021.get("EXCHB1");
		String data = EXT021.get("EXDATA");

		String entryDate = EXT021.get("EXRGDT");
		String entryTime = EXT021.get("EXRGTM");
		String changeDate = EXT021.get("EXLMDT");
		String changeNumber = EXT021.get("EXCHNO");
		String changedBy = EXT021.get("EXCHID");
		String tx60 = EXT021.get("EXTX60");

		mi.outData.put("CONO", cono);
		mi.outData.put("CUNO", cuno);
		mi.outData.put("ASCD", ascd);
		mi.outData.put("FDAT", fdat);
		mi.outData.put("TYPE", type);
		mi.outData.put("CHB1", chb1);
		mi.outData.put("DATA", data);
		mi.outData.put("TX60", tx60);
		mi.outData.put("RGDT", entryDate);
		mi.outData.put("RGTM", entryTime);
		mi.outData.put("LMDT", changeDate);
		mi.outData.put("CHNO", changeNumber);
		mi.outData.put("CHID", changedBy);
		mi.write();
	}
}
