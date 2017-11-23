using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Npgsql;
using System.Net;
using System.IO;
using System.Security.Cryptography;
using System.Xml;
using System.Globalization;


namespace MicroPhocas
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger
            (System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static NpgsqlConnection conn;

        static string csvDataPath = "C:\\Phocas\\Rawdata\\";


        static void Main(string[] args)
        {

            ConnectToDB();

            DateTime start = DateTime.Now;

            Console.WriteLine("Beginning data extract at " + start);
            log.Info("Beginning data extract at " + start);
            
            // Generate Site CSV
            GetSiteCSV();

            // Generate Sale CSV
            GetSaleCSV();

            // Generate Detailed SaleResult CSV
            GetDetailedSaleResultCSV();

            // Generate Relevant Vehicle CSV
            GetVehicleCSV();

            DateTime end = DateTime.Now;
            Console.WriteLine("Completed data extract " + end);
            log.Info("Completed data extract " + end);

            TimeSpan duration = end - start;

            Console.WriteLine("Time taken " + duration.ToString(@"hh\:mm\:ss"));
            log.Info("Completed data extract " + duration.ToString(@"hh\:mm\:ss"));

        }

        static public string DecodeFrom64(string encodedData)
        {
            byte[] encodedDataAsBytes
                = System.Convert.FromBase64String(encodedData);
            string returnValue =
               System.Text.ASCIIEncoding.ASCII.GetString(encodedDataAsBytes);
            return returnValue;
        }

        static public void LogMsg(string info)
        {
            String[] logmsg = { info };

            log.Info(logmsg);

            return;
        }

        static public void LogMsg(Exception e)
        {
            String[] logmsg = { e.Message };

            log.Fatal(logmsg, e);

            return;
        }



        /**
         * Created on 02/11/2016.
         * Sets up Postgrsql database connection using npgsql
         * @author andy
         *
         */
        private static void ConnectToDB()
        {
            // Login to PostgreSQL
            string[] lines = null;
            string sqlconnection;
            string password;
            try
            {
                lines = System.IO.File.ReadAllLines(@"C:\\Users\\AMS\\dbadmin.cfg");
            }
            catch (IOException ie)
            {
                Console.Write("Please check config file");
                log.Warn("Please check config file " + ie.Message);
                return;
            }

            password = DecodeFrom64(lines[4]);

            sqlconnection = "Server=" + lines[0] + ";Port=" + lines[1] + ";Database=" + lines[2] + ";User Id=" + lines[3] + ";Password=" + password + ";" + "CommandTimeout=1440;";

            conn = new NpgsqlConnection(sqlconnection);

            return;
        }


        /**
        * Created on 02/11/2016.
        * Writes the contents of a DataReader to a .csv file line by line for Phocas
        * @author andy
        *
        */
        private static StreamWriter WriteCSV(string fn, NpgsqlDataReader dr)
        {
            StringBuilder sb = new StringBuilder();
            using (StreamWriter writetext = new StreamWriter(fn))
            {
                var columnNames = Enumerable.Range(0, dr.FieldCount).Select(dr.GetName).ToList();
                sb.AppendLine(string.Join(",", columnNames));
                writetext.Write(sb.ToString());
                while (dr.Read())
                {
                    var fields = Enumerable.Range(0, dr.FieldCount).Select(dr.GetValue).ToList();
                    sb.Clear();
                    sb.AppendLine(string.Join(",", fields.Select(field => string.Concat("\"", field.ToString().Replace("\"", "\"\""), "\""))));
                    writetext.Write(sb.ToString());
                }
                return writetext;
            }
        }

        /**
        * Created on 23/05/2017.
        * Gets site data for Phocas
        * @author andy
        *
        */
        private static void GetSiteCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all sites
                sql = "SELECT ";
                sql += "site.\"id\" AS site_id,";
                sql += "site.\"name\" AS site_name,";
                sql += "site.\"shortname\" AS site_shortname,";
                sql += "site.\"address_postcode\" AS site_address_postcode";
                sql += " FROM ";
                sql += "site";
                sql += " ORDER BY site.\"id\"";

                LogMsg("Site SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Site Data");
                LogMsg("Extracted Site Data");


                String fn = csvDataPath + "microsites" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Site Data");
                LogMsg("Written Site Data");

            }

            catch (NpgsqlException ne)
            {
                Console.WriteLine("SQL Error {0}", ne.Message);
                LogMsg(ne);
            }

            catch (IOException ie)
            {
                Console.WriteLine("IOException Error {0}", ie.Message);
                LogMsg(ie);
            }
            catch (WebException we)
            {
                Console.WriteLine("Upload File Failed, status {0}", we.Message);
                LogMsg(we);
            }

            finally
            {
                conn.Close();
            }

        }

        /**
         * Created on 02/11/2016.
         * Gets sale data for Phocas
         * @author andy
         *
         */
        private static void GetSaleCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Sales
                sql = "SELECT ";
                sql += "sale.id AS Sale_id, ";
                sql += "sale.description AS sale_description, ";
                sql += "to_char(sale.start, 'dd/mm/yyyy') AS sale_start, ";
                sql += "sale.site_id AS sale_site_id, ";
                sql += "sale.hall_hall AS sale_hall, ";
                sql += "DATE(start) as dstart, ";
                sql += "case when EXTRACT(HOUR from start) < 12 then 'AM' else (case when EXTRACT(HOUR from start) < 16 then 'PM' else 'EVENING' end) end as hstart, ";
                sql += "initcap(to_char(start, 'dy')) as day, ";
                sql += "count(distinct buyer_id) as uniquebuyers, ";
                sql += "site.name as site_name ";
                sql += "FROM  ";
                sql += "sale ";
                sql += "INNER JOIN saleresult saleresult ON sale.id = saleresult.sale_id   ";
                sql += "INNER JOIN site site ON sale.site_id = site.id  ";
                sql += "WHERE sale.start >  'yesterday'";
                sql += "GROUP by sale.id, site.id ";
                sql += "ORDER BY sale.id, site.id ";

                LogMsg("Sale SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Sale Data");
                LogMsg("Extracted Sale Data");


                String fn = csvDataPath + "microsales" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written Sale Data");
                LogMsg("Written Sale Data");

            }

            catch (NpgsqlException ne)
            {
                Console.WriteLine("SQL Error {0}", ne.Message);
                LogMsg(ne);
            }

            catch (IOException ie)
            {
                Console.WriteLine("IOException Error {0}", ie.Message);
                LogMsg(ie);
            }
            catch (WebException we)
            {
                Console.WriteLine("Upload File Failed, status {0}", we.Message);
                LogMsg(we);
            }

            finally
            {
                conn.Close();
            }

        }

        /**
 * Created on 03/11/2016.
 * Gets enhanced sale result data for Phocas
 * @author andy
 *
 */
        private static void GetDetailedSaleResultCSV()
        {
            try
            {
                conn.Open();
                string sql = null;

                // Find all Detailed Sale Results
                sql = "SELECT ";
                sql += "min(sale.site_id) as sale_site_id,  ";
                sql += "min(sale.id) as sale_id,    ";
                sql += "min(to_char(sale.start, 'dd/mm/yyyy')) as sale_start,    ";
                sql += "min(sale.description) as sale_description,    ";
                sql += "min(case when saleresult.lot is null then 0 else saleresult.lot end) as saleresult_lot,    ";
                sql += "min(saleresult.status) as saleresult_status,    ";
                sql += "min(case when saleresult.status = 1 then saleresult.closingprice else 0 end) as saleresult_closingprice,    ";
                sql += "min(case when saleresult.status = 1 then saleresult.salemethod else '' end) as saleresult_method,    ";
                sql += "min(vehicle.registration) as vehicle_registration,    ";
                sql += "min(vehicle.id) as vehicle_id,    ";
                sql += "min(vehicle.calculatedpricing_clean) as vehicle_calculatedpricing_clean,    ";
                sql += "min(vehicle.pricing_reserveprice) as vehicle_pricing_reserveprice,    ";
                sql += "min(seller.accountnumber) as seller_accountnumber,    ";
                sql += "min(seller.name) as seller_name,    ";
                sql += "min(buyer.accountnumber) as buyer_accountnumber,    ";
                sql += "min(buyer.name) as buyer_name,    ";
                sql += "min(case when vehicle.calculatedpricing_clean is not null and saleresult.status = 1 then saleresult.closingprice else 0.0 end) as soldcapclean,    ";
                sql += "min(case when vehicle.pricing_reserveprice is not null  and saleresult.status = 1 then saleresult.closingprice else 0.0 end) as soldreserve,    ";
                sql += "min(case when saleresult.closingprice is not null and saleresult.status = 1 then vehicle.pricing_reserveprice else 0.0 end) as reserveclosing,    ";
                sql += "min(case when saleresult.closingprice is not null and saleresult.status = 1 then vehicle.calculatedpricing_clean else 0.0 end) as soldclosing,    ";
                sql += "min(case when vehicle.calculatedpricing_average is not null and saleresult.status = 1 then saleresult.closingprice else 0.0 end) as soldcapaverage,   ";
                sql += "min(case when saleresult.closingprice is not null and saleresult.status = 1 then vehicle.calculatedpricing_average else 0.0 end) as capaveragesold,    ";
                sql += "min(seller.id) as seller_id,    ";
                sql += "min(buyer.id) as buyer_id,    ";
                sql += "min(to_char(saleresult.soldstamp, 'dd/mm/yyyy')) as saleresult_soldstamp,    ";
                sql += "min(case when saleresult.webviews is null then 0 else saleresult.webviews end) as saleresult_webviews,    ";
                sql += "min(case when saleresult.uniquewebviews is null then 0 else saleresult.uniquewebviews end) as saleresult_uniquewebviews,    ";
                sql += "min(case when vehicle.damagecost is not null and saleresult.status = 1 then vehicle.damagecost else case when inspection.totaldamage is not null then inspection.totaldamage else 0 end end) as vehicledamage,   ";
                sql += "min(case when vehicle.damagecost is not null and saleresult.status = 1 then 1 else case when inspection.totaldamage is not null then 1 else 0 end end) as vehicledamagecount,   ";
                sql += "min(case when vehicle.mileage is not null and saleresult.status = 1 then vehicle.mileage else 0 end) as soldvehiclemileage,   ";
                sql += "min(case when vehicle.mileage is not null and saleresult.status = 1 then 1 else 0 end) as soldvehiclemileagecount,   ";
                sql += "min(sales_per_vehicle.count) as sales_per_vehicle,   ";
                sql += "min(extract('days' from (NOW() - vehicle.firstregistration))) as age,   ";
                sql += "min(inspection.grade) as grade,   ";
                sql += "min(case when saleresult.status = 0 then 1 else 0 end) as enteredcount,   ";
                sql += "min(case when saleresult.status = 1 then 1 else 0 end) as soldcount,   ";
                sql += "min(case when saleresult.status = 2 then 1 else 0 end) as unsoldcount,   ";
                sql += "min(case when saleresult.status = 3 then 1 else 0 end) as provisionalcount,  ";
                sql += "max(case when saleresult.status = 1 and sales_per_vehicle.count = 1 then 1 else 0 end) as firsttimesale,   ";
                sql += "min(case when vehicle.onhold is true then 1 else 0 end) as onholdcount,   ";
                sql += "min(case when vehicle.withdrawn is true then 1 else 0 end) as withdrawncount,   ";
                sql += "min(case when vehicle.exitdate is not null then ((EXTRACT(epoch from age(vehicle.exitdate, vehicle.entrydate)) / 86400)::int) else ((EXTRACT(epoch from age(NOW(), vehicle.entrydate)) / 86400)::int) end) AS daysonsite, ";
                sql += "min(case when saleresult.salemethod = 'Physical' then 1 else 0 end) as physicalcount,   ";
                sql += "min(case when saleresult.salemethod = 'Online' then 1 else 0 end) as onlinecount,   ";
                sql += "min(case when saleresult.salemethod = 'BidBuyNow' then 1 else 0 end) as bidbuynowcount,   ";
                sql += "min(abs(EXTRACT(EPOCH from (stamp - entrydate)) / 60))::integer as timetoweb,      ";
                sql += "min(case when inspection.grade is null and vehicle.exitdate is null and vehicle.withdrawn is false and vehicle.onhold is false then 0 else 1 end) as inspected,    ";
                sql += "min(bive_ext.total_charges_net) as buyerchargesnet,  ";
                sql += "min(bive_ext.total_charges_gross) as buyerchargesgross  ";
                sql += "FROM    ";
                sql += "public.vehicle vehicle INNER JOIN public.client seller ON seller.id = vehicle.seller_id    ";
                sql += "INNER JOIN public.saleresult saleresult ON vehicle.id = saleresult.vehicle_id    ";
                sql += "INNER JOIN public.sale sale ON saleresult.sale_id = sale.id    ";
                sql += "LEFT OUTER JOIN history on vehicle.id = history.vehicle_id and history.text like '%Edited vehicle%'   ";
                sql += "LEFT OUTER JOIN public.client buyer ON saleresult.buyer_id = buyer.id    ";
                sql += "LEFT OUTER JOIN public.inspection inspection ON vehicle.primaryinspection_id = inspection.id    ";
                sql += "LEFT OUTER JOIN public.sales_per_vehicle sales_per_vehicle ON vehicle.id = sales_per_vehicle.vehicle_id    ";
                sql += "LEFT OUTER JOIN public.buyerinvoicevehicleentry bive ON bive.vehicle_id = vehicle.id and bive.rescinded = false and saleresult.status = 1 ";
                sql += "LEFT OUTER JOIN public.bive_extended bive_ext on bive.id = bive_ext.id ";
                sql += "WHERE   ";
                sql += "vehicle.vatstatus is not null and vehicle.make is not null  ";
                sql += "and vehicle.entrydate is not null   ";
                sql += "and sale.start > 'yesterday'  ";
                sql += "group by sale.id, saleresult.lot  ";
                sql += "order by sale.id, saleresult.lot  ";

                LogMsg("Detailed SaleResult SQL " + sql);

                NpgsqlCommand command = new NpgsqlCommand(sql, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted detailed saleresult Data");
                LogMsg("Extracted detailed saleresult Data");

                String fn = csvDataPath + "detailedmicrosaleresults" + ".csv";

                WriteCSV(fn, dr);

                Console.WriteLine("Written detailed saleresult Data");
                LogMsg("Written detailed saleresult Data");
            }

            catch (NpgsqlException ne)
            {
                Console.WriteLine("SQL Error {0}", ne.Message);
                LogMsg(ne);
            }

            catch (IOException ie)
            {
                Console.WriteLine("IOException Error {0}", ie.Message);
                LogMsg(ie);
            }
            catch (WebException we)
            {
                Console.WriteLine("Upload File Failed, status {0}", we.Message);
                LogMsg(we);
            }

            finally
            {
                conn.Close();
            }

        }

        /**
 * Created on 02/11/2016.
 * Gets vehicle data for Phocas
 * @author andy
 *
 */
        private static void GetVehicleCSV()
        {
            try
            {
                conn.Open();
                string sql = null;
                string imgixurl = "https://abimg002.imgix.net/";

                // Find all vehicles
                sql = "SELECT DISTINCT on (vehicle.\"id\") ";
                sql += "vehicle.\"id\" AS vehicle_id,";
                sql += "vehicle.\"bodystyle\" AS vehicle_bodystyle,";
                sql += "CASE ";
                sql += "WHEN bodystyle like '%Cabriolet%' or bodystyle like '%CABRIOLET%' then 'Cabriolet' ";
                sql += "WHEN bodystyle like '%Convertible%' or bodystyle like '%CONVERTIBLE%' then 'Convertible' ";
                sql += "WHEN bodystyle like '%Coupe%' or bodystyle like '%CABRIOLET%' then 'Coupe' ";
                sql += "WHEN bodystyle = 'Double Cab Pick-up' or bodystyle = 'Double Cab Dropside' or bodystyle = 'Double Cab Tipper' or bodystyle = 'Double Chassis Cab' then 'Double Cab Pick-up' ";
                sql += "WHEN bodystyle like '%Estate%' or bodystyle like '%ESTATE%' then 'Estate' ";
                sql += "WHEN bodystyle like '%Hardtop%' then 'Cabriolet' ";
                sql += "WHEN bodystyle like '%Hatchback%' or bodystyle like '%HATCHBACK%' then 'Hatchback' ";
                sql += "WHEN bodystyle like '%High Volume/High Roof Van%' then 'High Volume/High Roof Van' ";
                sql += "WHEN bodystyle = 'Medium Roof Van' then 'Medium Roof Van' ";
                sql += "WHEN bodystyle like '%Roadster%' then 'Roadster' ";
                sql += "WHEN bodystyle like '%Saloon%' or bodystyle like '%SALOON%' then 'Saloon' ";
                sql += "WHEN bodystyle like '%Station Wagon%' then 'Station Wagon' ";
                sql += "WHEN bodystyle like '%Van%' then 'Van' ";
                sql += "ELSE 'Others' ";
                sql += "END AS vehicle_standard_bodystyle,";
                sql += "vehicle.\"colour\" AS vehicle_colour,";
                sql += "case when vehicle.\"doors\" is null then 0 else vehicle.\"doors\" end AS vehicle_doors,";
                sql += "CASE ";
                sql += "WHEN doors = 2 then '2 doors' ";
                sql += "WHEN doors = 3 then '3 doors' ";
                sql += "WHEN doors = 4 then '4 doors' ";
                sql += "WHEN doors = 5 then '5 doors' ";
                sql += "ELSE 'Other' ";
                sql += "END AS vehicle_doors_band, ";
                sql += "to_char(vehicle.\"entrydate\", 'dd/mm/yyyy') AS vehicle_entrydate,";
                sql += "to_char(vehicle.\"firstregistration\", 'dd/mm/yyyy') AS vehicle_firstregistration,";
                sql += "CASE ";
                sql += "WHEN firstregistration between NOW() - INTERVAL '30 Month' and NOW() then 'Late & Low' ";
                sql += "WHEN firstregistration between NOW() - INTERVAL '54 Month' and NOW() then 'Fleet Profile' ";
                sql += "WHEN firstregistration between NOW() - INTERVAL '78 Month' and NOW() then 'PX Young' ";
                sql += "WHEN firstregistration between NOW() - INTERVAL '126 Month' and NOW() then 'PX Old' ";
                sql += "else 'Budget' end AS vehicle_age,";
                sql += "vehicle.\"fuel\" AS vehicle_fuel, ";
                sql += "CASE ";
                sql += "WHEN vehicle.fuel like '%Hybrid%' or vehicle.fuel like '%HYB%' then 'Hybrid' ";
                sql += "WHEN vehicle.fuel = 'Diesel' or vehicle.fuel = 'DIESEL' then 'Diesel' ";
                sql += "WHEN vehicle.fuel = 'Electric' or vehicle.fuel = 'ELECTRIC' then 'Electric' ";
                sql += "WHEN vehicle.fuel = 'petrol' or vehicle.fuel = 'Petrol' or vehicle.fuel = 'PETROL' or vehicle.fuel like '%Petrol/Bio-Ethanol%'then 'Petrol' ";
                sql += "WHEN vehicle.fuel = 'Petrol/ELE' or vehicle.fuel = 'Petrol/Gas' or vehicle.fuel = 'PETROL/GAS' or vehicle.fuel = 'Petrol/LPG' or vehicle.fuel = 'PETROL/ELE' then 'Petrol'	 ";
                sql += "ELSE 'Others' ";
                sql += "END AS FuelType, ";
                sql += "case when capcoding.\"manufacturer\" is not null then capcoding.\"manufacturer\" else vehicle.make end AS vehicle_make,";
                //                sql += "vehicle.make AS vehicle_make,";
                sql += "case when vehicle.\"mileage\" is null then 1 else vehicle.\"mileage\" end AS vehicle_mileage,";
                sql += "CASE ";
                sql += "WHEN vehicle.mileage is null then 'Unknown'";
                sql += "WHEN vehicle.mileage <= 1000 then 'Up to 1,000'";
                sql += "WHEN vehicle.mileage > 1000 and vehicle.mileage <= 5000 then 'Up to 5,000'";
                sql += "WHEN vehicle.mileage > 5000 and vehicle.mileage <= 10000 then 'Up to 10,000'";
                sql += "WHEN vehicle.mileage > 10000 and vehicle.mileage <= 20000 then 'Up to 20,000'";
                sql += "WHEN vehicle.mileage > 20000 and vehicle.mileage <= 30000 then 'Up to 30,000'";
                sql += "WHEN vehicle.mileage > 30000 and vehicle.mileage <= 40000 then 'Up to 40,000'";
                sql += "WHEN vehicle.mileage > 40000 and vehicle.mileage <= 50000 then 'Up to 50,000'";
                sql += "WHEN vehicle.mileage > 50000 and vehicle.mileage <= 60000 then 'Up to 60,000'";
                sql += "WHEN vehicle.mileage > 60000 and vehicle.mileage <= 70000 then 'Up to 70,000'";
                sql += "WHEN vehicle.mileage > 70000 and vehicle.mileage <= 80000 then 'Up to 80,000'";
                sql += "WHEN vehicle.mileage > 80000 and vehicle.mileage <= 90000 then 'Up to 90,000'";
                sql += "WHEN vehicle.mileage > 90000 and vehicle.mileage <= 100000 then 'Up to 100,000'";
                sql += "ELSE 'Over 100,000'";
                sql += "END as Mileage_Band,";
                sql += "case when capcoding.\"longmodel\" is not null then capcoding.\"longmodel\" else vehicle.model end AS vehicle_model, ";
                //                sql += "vehicle.model AS vehicle_model, ";
                sql += "to_char(vehicle.\"motexpiry\", 'dd/mm/yyyy') AS vehicle_motexpiry,";
                sql += "vehicle.\"previouskeepers\" AS vehicle_previouskeepers,";
                sql += "vehicle.\"previousregistration\" AS vehicle_previousregistration,";
                sql += "vehicle.\"registration\" AS vehicle_registration,";
                sql += "to_char(vehicle.\"taxexpiry\", 'dd/mm/yyyy') AS vehicle_taxexpiry,";
                sql += "vehicle.\"v5heldstate\" AS vehicle_v5heldstate,";
                sql += "vehicle.\"version\" AS vehicle_version,";
                sql += "vehicle.\"vin\" AS vehicle_vin,";
                sql += "vehicle.\"capcode\" AS vehicle_capcode,";
                sql += "vehicle.\"calculatedpricing_average\" AS vehicle_calculatedpricing_average,";
                sql += "vehicle.\"calculatedpricing_belowaverage\" AS vehicle_calculatedpricing_belowaverage,";
                sql += "vehicle.\"calculatedpricing_clean\" AS vehicle_calculatedpricing_clean,";
                sql += "vehicle.\"calculatedpricing_retail\" AS vehicle_calculatedpricing_retail,";
                sql += "vehicle.\"pricing_closingprice\" AS vehicle_pricing_closingprice,";
                sql += "vehicle.\"pricing_finalprice\" AS vehicle_pricing_finalprice,";
                sql += "vehicle.\"pricing_reserveprice\" AS vehicle_pricing_reserveprice,";
                sql += "vehicle.\"autoreserve\" AS vehicle_autoreserve,";
                sql += "vehicle.\"lastresult_sale_id\" AS vehicle_lastresult_sale_id,";
                sql += "vehicle.\"lastresult_vehicle_id\" AS vehicle_lastresult_vehicle_id,";
                sql += "vehicle.\"longderivative\" AS vehicle_longderivative,";
                sql += "vehicle.\"mileagewarranty\" AS vehicle_mileagewarranty,";
                sql += "vehicle.\"servicehistory\" AS vehicle_servicehistory,";
                sql += "vehicle.\"taxexpired\" AS vehicle_taxexpired,";
                sql += "vehicle.\"soldasseen\" AS vehicle_soldasseen,";
                sql += "case when vehicle.\"transmission\" is null then 'N/A' else vehicle.\"transmission\" end AS vehicle_transmission,";
                sql += "vehicle.\"vatstatus\" AS vehicle_vatstatus,";
                sql += "CASE ";
                sql += "WHEN vatstatus = 0 then 'Qualifying' ";
                sql += "WHEN vatstatus = 1 then 'Margin' ";
                sql += "WHEN vatstatus = 2 then 'Commerical subj. to VAT' ";
                sql += "WHEN vatstatus = 3 then 'Commercial no VAT' ";
                sql += "END AS Vat, ";
                sql += "vehicle.\"remarks\" AS vehicle_remarks,";
                sql += "vehicle.\"experiantotalloss\" AS vehicle_experiantotalloss,";
                sql += "vehicle.\"glasstradeprice\" AS vehicle_glasstradeprice,";
                sql += "vehicle.\"lastserviced\" AS vehicle_lastserviced,";
                sql += "vehicle.\"extraspec\" AS vehicle_extraspec,";
                sql += "case when vehicle.\"co2emission\" is null then 0 else vehicle.\"co2emission\" end AS vehicle_co2emission,";
                sql += "vehicle.\"yearofmanufacture\" AS vehicle_yearofmanufacture,";
                sql += "vehicle.\"damagecost\" AS vehicle_damagecost,";
                sql += "vehicle.\"buyitnow\" AS vehicle_buyitnow,";
                sql += "vehicle.\"excludefromlivebid\" AS vehicle_excludefromlivebid,";
                sql += "vehicle.\"excludefromwebsite\" AS vehicle_excludefromwebsite,";
                sql += "vehicle.\"websupression\" AS vehicle_websupression,";
                sql += "vehicle.\"deltapoint_retail\" AS vehicle_deltapoint_retail,";
                sql += "vehicle.\"deltapoint_trade\" AS vehicle_deltapoint_trade, ";
                sql += "vehicle.\"enginesizecc\" AS vehicle_enginesizecc, ";
                sql += "CASE ";
                sql += "WHEN enginesizecc is null then 'Unknown'";
                sql += "WHEN enginesizecc between 0 and 999 then 'Less than 1.0L' ";
                sql += "WHEN enginesizecc between 1000 and 1399 then '1.0L - 1.3L' ";
                sql += "WHEN enginesizecc between 1400 and 1699 then '1.4L - 1.6L' ";
                sql += "WHEN enginesizecc between 1700 and 1999 then '1.7L - 1.9L' ";
                sql += "WHEN enginesizecc between 2000 and 2599 then '2.0L - 2.5L' ";
                sql += "WHEN enginesizecc between 2600 and 2999 then '2.6L - 2.9L' ";
                sql += "WHEN enginesizecc between 3000 and 3999 then '3.0L - 3.9L' ";
                sql += "WHEN enginesizecc between 4000 and 4999 then '4.0L - 4.9L' ";
                sql += "ELSE 'Over 5.0L' ";
                sql += "END AS vehicle_enginesize_band, ";
                sql += "vehicle.\"plant\" AS vehicle_plant, ";
                sql += "to_char(vehicle.\"exitdate\", 'dd/mm/yyyy') AS vehicle_exitdate, ";
                sql += "vehicle.\"site_id\" AS vehicle_site_id, ";
                sql += "sales_per_vehicle.\"count\" AS sales_per_vehicle_count, ";
                sql += "case when inspection.\"grade\" is null or LENGTH(inspection.grade) < 1 then  'N/A' else inspection.\"grade\" end AS inspection_grade, ";
                sql += "case when inspection.\"result\" is null or LENGTH(inspection.result) < 1 then 'N/A' else inspection.\"result\" end AS inspection_result, ";
                sql += "case when inspection.\"totaldamage\" is null then 0 else inspection.\"totaldamage\" end AS inspection_totaldamage, ";
                sql += "case when inspection.\"nama\" is null or LENGTH(inspection.nama) < 1 then 'N/A' else inspection.\"nama\" end AS inspection_nama, ";
                sql += "case when vehicle.exitdate is not null then ((EXTRACT(epoch from age(vehicle.exitdate, vehicle.entrydate)) / 86400)::int) else 0 end AS daysonsite, ";
                sql += "case when vehicle.assured_id is null then 'Not Assured' else 'Assured' end AS vehicle_assured, ";
                sql += "case when vehicle.colour is null then 'Not Specified' ";
                sql += "when lower(vehicle.colour) like '%black%' then 'Black' ";
                sql += "when lower(vehicle.colour) like '%white%' then 'White' ";
                sql += "when lower(vehicle.colour) like '%silver%' then 'Silver' ";
                sql += "when lower(vehicle.colour) like '%red%' then 'Red' ";
                sql += "when lower(vehicle.colour) like '%blue%' then 'Blue' ";
                sql += "when lower(vehicle.colour) like '%green%' then 'Green' ";
                sql += "when lower(vehicle.colour) like '%yellow%' then 'Yellow' ";
                sql += "when lower(vehicle.colour) like '%gold%' then 'Gold' ";
                sql += "when lower(vehicle.colour) like '%bronze%' then 'Bronze' ";
                sql += "when lower(vehicle.colour) like '%purple%' then 'Purple' ";
                sql += "when lower(vehicle.colour) like '%magenta%' then 'Magenta' ";
                sql += "when lower(vehicle.colour) like '%grey%' then 'Grey' ";
                sql += "when lower(vehicle.colour) like '%brown%' then 'Brown' ";
                sql += "when lower(vehicle.colour) like '%beige%' then 'Beige' ";
                sql += "when lower(vehicle.colour) like '%fire%' then 'Red' ";
                sql += "when lower(vehicle.colour) like '%anthracite%' then 'Silver' ";
                sql += "when lower(vehicle.colour) like '%cream%' then 'Cream' ";
                sql += "when lower(vehicle.colour) like '%maroon%' then 'Maroon' ";
                sql += "when lower(vehicle.colour) like '%violet%' then 'Violet' ";
                sql += "when lower(vehicle.colour) like '%mauve%' then 'Mauve' ";
                sql += "when lower(vehicle.colour) like '%orange%' then 'Orange' ";
                sql += "when lower(vehicle.colour) like '%turquoise%' then 'Turquoise' ";
                sql += "when lower(vehicle.colour) like '%platinum%' then 'Silver' ";
                sql += "when lower(vehicle.colour) like '%graphite%' then 'Grey'  ";
                sql += "when lower(vehicle.colour) like '%venetian%' then 'Red'  ";
                sql += "when lower(vehicle.colour) like '%ruby%' then 'Red'  ";
                sql += "when lower(vehicle.colour) like '%multi%' then 'Multi-Coloured'  ";
                sql += "when lower(vehicle.colour) like '%pink%' then 'Pink' else 'Other'  ";
                sql += "end AS vehicle_standard_colour, ";
                sql += "case when inspection.grade is null or LENGTH(inspection.grade) < 1 or inspection.result is null or LENGTH(inspection.result) < 1 then 'N/A' ";
                sql += " else (concat(inspection.grade,left(inspection.result,1))) end AS combined_grade, ";
                sql += "case when inspection.costedreport_id is null then '' else '" + imgixurl + "' || image.externalpath end as costedpdfurl, ";
                sql += "case when vehicle.withdrawn is true then 1 else 0 end as withdrawn, ";
                sql += "case when (inspection.date is not null and vehicle.entrydate is not null) then ((EXTRACT(epoch from age(inspection.date, vehicle.entrydate)) / 60)::int) else 0 end AS time_to_inspect, ";
                sql += "inspection.inspector as inspection_inspector, ";
                sql += "inspection.provider as inspection_provider, ";
                sql += "case when transportrecord.inspectedoffsite is true then 1 else 0 end as inspectedoffsite, ";
                sql += "case when vehicle.onhold is true then 1 else 0 end as onholdcount,   ";
                sql += "case when vehicle.exitdate is null and vehicle.entrydate is not null and   ";
                sql += "((saleresult.status <> 1 and sales_per_vehicle.count > 5) or   ";
                sql += "(vehicle.onhold is true and ((NOW()::date - vehicle.entrydate::date) > 14)) or   ";
                sql += "(saleresult.status = 1 and ((NOW()::date - saleresult.soldstamp::date) > 14)) or   ";
                sql += "(vehicle.withdrawn is true) or   ";
                sql += "(vehicle.entrydate is not null and vehicle.lastresult_sale_id is null and ((NOW()::date - vehicle.entrydate::date) > 14)))  then 1 else 0 end as naughty  ";
                sql += "FROM ";
                sql += "\"public\".\"vehicle\" vehicle INNER JOIN \"public\".\"sales_per_vehicle\" sales_per_vehicle ON vehicle.\"id\" = sales_per_vehicle.\"vehicle_id\" ";
                sql += "LEFT OUTER JOIN \"public\".\"inspection\" inspection ON vehicle.\"primaryinspection_id\" = inspection.\"id\"   ";
                sql += "LEFT OUTER JOIN public.image image ON inspection.costedreport_id = image.id   ";
                sql += "LEFT OUTER JOIN public.transportrecord transportrecord ON transportrecord.vehicle_id = vehicle.id   ";
                sql += "LEFT OUTER JOIN public.capcoding capcoding ON capcoding.vehiclecode = vehicle.capcode ";
                sql += "LEFT OUTER JOIN public.saleresult saleresult ON saleresult.vehicle_id = vehicle.id and saleresult.sale_id = vehicle.lastresult_sale_id ";
                sql += "INNER JOIN \"public\".\"sale\" sale ON saleresult.\"sale_id\" = sale.\"id\"   ";
                sql += " WHERE";
                sql += " vehicle.\"vatstatus\" is not null and vehicle.\"make\" is not null";
                sql += " and vehicle.\"entrydate\" is not null ";
                sql += " and sale.start > 'yesterday' ";

                String order = " ORDER BY vehicle.\"id\"";
                String query = sql + order;

                LogMsg("Vehicle SQL " + query);

                NpgsqlCommand command = new NpgsqlCommand(query, conn);
                NpgsqlDataReader dr = command.ExecuteReader();

                Console.WriteLine("Extracted Vehicle Data 1");
                LogMsg("Extracted Vehicle Data 1");

                String fn = csvDataPath + "microvehicles" + ".csv";

                StreamWriter sr = WriteCSV(fn, dr);

                Console.WriteLine("Written Vehicle Data 1");
                LogMsg("Written Vehicle Data 1");

            }

            catch (NpgsqlException ne)
            {
                Console.WriteLine("SQL Error {0}", ne.Message);
                LogMsg(ne);
            }

            catch (IOException ie)
            {
                Console.WriteLine("IOException Error {0}", ie.Message);
                LogMsg(ie);
            }
            catch (WebException we)
            {
                Console.WriteLine("Upload File Failed, status {0}", we.Message);
                LogMsg(we);
            }

            finally
            {
                conn.Close();
            }

        }



    }
}
