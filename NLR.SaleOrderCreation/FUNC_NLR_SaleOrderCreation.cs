using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;

namespace NLR.SaleOrderCreation
{
   

    public static class FUNC_NLR_SaleOrderCreation
    {
        private static readonly string _organizationUrl;
        private static readonly string _appId;
        private static readonly string _secretId;
        private static readonly bool _useTodayDate;
        private static readonly DateTime _customDateFrom;
        private static readonly DateTime _customDateTo;

        private static readonly string _scheduleTriggerTime;
        private static ILogger _logger;

      

        static FUNC_NLR_SaleOrderCreation()
        {
            _organizationUrl = System.Environment.GetEnvironmentVariable("OrganizationUrl");
            _appId = System.Environment.GetEnvironmentVariable("AppId");
            _secretId = System.Environment.GetEnvironmentVariable("SecretId");
            _scheduleTriggerTime = System.Environment.GetEnvironmentVariable("ScheduleTriggerTime");
            _useTodayDate = Convert.ToBoolean(System.Environment.GetEnvironmentVariable("UseTodayDate"));
            if (!_useTodayDate)
            {
                _customDateFrom = Convert.ToDateTime(System.Environment.GetEnvironmentVariable("CustomDateFrom"));
                _customDateTo = Convert.ToDateTime(System.Environment.GetEnvironmentVariable("CustomDateTo"));
            }

          
    }

        [Timeout("02:00:00")]
        [FunctionName("FUNC_NLR_SaleOrderCreation")]
        public static void Run([TimerTrigger("%ScheduleTriggerTime%")] TimerInfo myTimer, ILogger logger)
        {
            logger.LogInformation("FUNC_NLR_SaleOrderCreation - Started");
            _logger = logger;

            if (_useTodayDate)
            {
                // StartOperation(DateTime.Now.AddDays(-1));
                StartOperation(DateTime.UtcNow);
            }
            else
            {
                foreach (DateTime day in EachDay(_customDateFrom, _customDateTo))
                {
                    StartOperation(day);
                }
            }

        }

        static IEnumerable<DateTime> EachDay(DateTime from, DateTime thru)
        {
            for (var day = from.Date; day.Date <= thru.Date; day = day.AddDays(1))
                yield return day;
        }

        static void StartOperation(DateTime date)
        {
            try
            {
                _logger.LogInformation($"FUNC_NLR_SaleOrderCreation - Date: {date}");
                FunctionHelper.accountsKeyPairs.Clear();
                FunctionHelper.contactsKeyPairs.Clear();
                FunctionHelper.ordersKeyPairs.Clear();
                FunctionHelper.businessUnitsKeyPairs.Clear();

                FunctionHelper.businessUnitDictionary.Clear();


                string dateParam = string.Empty;

                var accountsList = new List<Account>();
                var contactsList = new List<Contact>();
                var ordersList = new List<Order>();
                var ordersDetailList = new List<OrderDetail>();

                    var service = ConnectToMSCRM();

                //                string fetchXml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                //  <entity name='sig_smig_productsaleshistory'>
                //    <attribute name='sig_smig_productsaleshistoryid' />
                //    <attribute name='sig_orderid' />
                //    <attribute name='createdon' />
                //    <attribute name='sig_workflow' />
                //    <attribute name='sig_units' />
                //    <attribute name='sig_totalcharge' />
                //    <attribute name='sig_routename' />
                //    <attribute name='sig_producttype' />
                //    <attribute name='sig_product' />
                //    <attribute name='sig_postalcode' />
                //    <attribute name='sig_ordernumber' />
                //    <attribute name='sig_groupname' />
                //    <attribute name='sig_doctorname' />
                //    <attribute name='sig_doctorid' />
                //    <attribute name='sig_department' />
                //    <attribute name='sig_datecreated' />
                //    <attribute name='sig_corporatename' />
                //    <attribute name='sig_businessunitid' />
                //    <attribute name='sig_businessunitname' />
                //    <attribute name='sig_accountname' />
                //    <attribute name='sig_accountid' />
                //    <order attribute='sig_orderid' descending='false' />
                //    <filter type='and'>
                //      <condition attribute='modifiedon' operator='on' value='" + date + @"' />
                //    </filter>
                //  </entity>
                //</fetch>";

                string fetchXml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
                  <entity name='sig_easyrxproductsaleshistory'>
                    <attribute name='sig_easyrxproductsaleshistoryid' />
                    <attribute name='sig_orderid' />
                    <attribute name='createdon' />
                    <attribute name='sig_workflow' />
                    <attribute name='sig_units' />
                    <attribute name='sig_totalcharge' />
                    <attribute name='sig_routename' />
                    <attribute name='sig_producttype' />
                    <attribute name='sig_product' />
                    <attribute name='sig_postalcode' />
                    <attribute name='sig_ordernumber' />
                    <attribute name='sig_groupname' />
                    <attribute name='sig_doctorname' />
                    <attribute name='sig_doctorid' />
                    <attribute name='sig_department' />
                    <attribute name='sig_datecreated' />
                    <attribute name='sig_corporatename' />
                    <attribute name='sig_businessunitid' />
                    <attribute name='sig_businessunitname' />
                    <attribute name='sig_accountname' />
                    <attribute name='sig_accountid' />
                    <order attribute='sig_datecreated' descending='false' />
                    <filter type='and'>
                      <condition attribute='modifiedon' operator='on' value='" + date + @"' />
                    </filter>
                  </entity>
                </fetch>";



                EntityCollection productData = service.RetrieveMultiple(new FetchExpression(fetchXml));

                    if (productData.Entities.Any())
                    {
                        _logger.LogInformation($"FUNC_NLR_SaleOrderCreation Total data count: {productData.Entities.Count}");
                    GetRegisteredApps(service);


                        foreach (var item in productData.Entities)
                        {
                            // getting accounts
                            var account = new Account()
                            {
                                accountid = Convert.ToString(item.GetAttributeValue<string>("sig_accountid")),
                                name = Convert.ToString(item.GetAttributeValue<string>("sig_accountname")),
                                PostalCode = Convert.ToString(item.GetAttributeValue<string>("sig_postalcode")),
                                sig_businessunit = Convert.ToString(item.GetAttributeValue<string>("sig_businessunitid")),
                                sig_corporatename = Convert.ToString(item.GetAttributeValue<string>("sig_corporatename")),
                                sig_workflow = Convert.ToString(item.GetAttributeValue<string>("sig_workflow")),
                                sig_groupname = Convert.ToString(item.GetAttributeValue<string>("sig_groupname")),
                                overriddencreatedon = Convert.ToString(item.GetAttributeValue<string>("sig_datecreated")),
                                sig_businessunitname = Convert.ToString(item.GetAttributeValue<string>("sig_businessunitname"))
                            };
                            accountsList.Add(account);


                            // getting contacts
                            var contact = new Contact()
                            {
                                sig_contactidentifier = Convert.ToString(item.GetAttributeValue<string>("sig_doctorid")),
                                sig_businessunit = Convert.ToString(item.GetAttributeValue<string>("sig_businessunitid")),
                                fullname = Convert.ToString(item.GetAttributeValue<string>("sig_doctorname")),
                                overriddencreatedon = Convert.ToString(item.GetAttributeValue<string>("sig_datecreated")),
                                sig_businessunitname = Convert.ToString(item.GetAttributeValue<string>("sig_businessunitname")),
                                accountid = Convert.ToString(item.GetAttributeValue<string>("sig_accountid"))
                            };
                            contactsList.Add(contact);

                            // getting orders

                            var order = new Order()
                            {
                                sig_saleorderid = Convert.ToString(item.GetAttributeValue<string>("sig_orderid")),
                                ordernumber = Convert.ToString(item.GetAttributeValue<string>("sig_ordernumber")),
                                overriddencreatedon = Convert.ToString(item.GetAttributeValue<string>("sig_datecreated")),
                                sig_routename = Convert.ToString(item.GetAttributeValue<string>("sig_routename")),
                                customerid = Convert.ToString(item.GetAttributeValue<string>("sig_doctorid")),
                                pricelevelid = "A3844CE7-C5DC-49E2-8475-70460D6F104C",
                                sig_businessunitname = Convert.ToString(item.GetAttributeValue<string>("sig_businessunitname"))
                            };
                            ordersList.Add(order);

                            // getting order details
                            var orderDetail = new OrderDetail()
                            {
                                sig_department = Convert.ToString(item.GetAttributeValue<string>("sig_department")),
                                quantity = item.GetAttributeValue<decimal>("sig_units"),
                                baseamount = item.GetAttributeValue<decimal>("sig_totalcharge"),
                                sig_producttype = Convert.ToString(item.GetAttributeValue<string>("sig_producttype")),
                                productdescription = Convert.ToString(item.GetAttributeValue<string>("sig_product")),
                                SaleOrderIdentifier = Convert.ToString(item.GetAttributeValue<string>("sig_orderid")),
                                sig_businessunitname = Convert.ToString(item.GetAttributeValue<string>("sig_businessunitname"))
                            };
                            ordersDetailList.Add(orderDetail);



                        }

                    var accountsGroupedByBusinessUnit = accountsList.GroupBy(x => x.sig_businessunitname).Select(x => x).ToList();
                    var contactsGroupedByBusinessUnit = contactsList.GroupBy(x => x.sig_businessunitname).Select(x => x).ToList();
                    var ordersGroupedByBusinessUnit = ordersList.GroupBy(x => x.sig_businessunitname).Select(x => x).ToList();
                    var orderDetailsGroupedByBusinessUnit = ordersDetailList.GroupBy(x => x.sig_businessunitname).Select(x => x).ToList();

                    // main operation
                    if (accountsList.Any())
                        {
                            var insertAccount = FunctionHelper.CreateAccounts(service, accountsList, _logger, accountsGroupedByBusinessUnit, _organizationUrl);
                            if (insertAccount)
                            {
                            _logger.LogInformation("FUNC_NLR_SaleOrderCreation - All Accounts created/updated successfully");
                            var insertContactsStatus = FunctionHelper.CreateContacts(service, contactsList, _logger, contactsGroupedByBusinessUnit, _organizationUrl);
                                if (insertContactsStatus)
                                {
                                _logger.LogInformation("FUNC_NLR_SaleOrderCreation - All Contacts created/updated successfully");
                                var ordersStatus = FunctionHelper.CreateOrders(service, ordersList, _logger, ordersGroupedByBusinessUnit, _organizationUrl);
                                    if (ordersStatus)
                                    {
                                    _logger.LogInformation("FUNC_NLR_SaleOrderCreation - All Orders created/updated successfully");
                                    var orderDetailCreationStatus = FunctionHelper.CreateOrderDetails(_logger, orderDetailsGroupedByBusinessUnit, _organizationUrl);
                                        if (orderDetailCreationStatus)
                                        {
                                        _logger.LogInformation("FUNC_NLR_SaleOrderCreation - All Sale Order lines created successfully");
                                        _logger.LogInformation("FUNC_NLR_SaleOrderCreation - Function Iteration Completed");
                                        }
                                        else
                                        {
                                            throw new Exception("Error in Order detail creation");
                                        }

                                    }
                                    else
                                    {
                                        throw new Exception("Error in Order creation");
                                    }

                                }
                                else
                                    throw new Exception("Error in Contacts creation");
                            }
                            else
                                throw new Exception("Error in Accounts creation");

                        }

                    }
                    else
                    {
                        _logger.LogInformation($"FUNC_NLR_SaleOrderCreation - Data not found in table sig_smig_productsaleshistory!");
                    }


            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        static void GetRegisteredApps(IOrganizationService service)
        {
            try
            {
                string fetchXml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
  <entity name='sig_configuration'>
    <attribute name='sig_name' />
    <attribute name='sig_value' />
    <attribute name='sig_key' />
    <filter type='and'>
      <condition attribute='sig_name' operator='like' value='BU%' />
    </filter>
  </entity>
</fetch>";

                EntityCollection businessUnitApps = service.RetrieveMultiple(new FetchExpression(fetchXml));
                if (!businessUnitApps.Entities.Any())
                    throw new Exception("No business unit app registration found in table sig_businessunitapp");

                foreach (var item in businessUnitApps.Entities)
                {
                    var appId = Convert.ToString(item.GetAttributeValue<string>("sig_key"));
                    var secretId = Convert.ToString(item.GetAttributeValue<string>("sig_value"));
                    var businessUnitName = Convert.ToString(item.GetAttributeValue<string>("sig_name"));
                    if (!string.IsNullOrEmpty(appId) && !string.IsNullOrEmpty(secretId) && !string.IsNullOrEmpty(businessUnitName))
                    {
                        FunctionHelper.businessUnitDictionary.Add(businessUnitName.Substring(2, businessUnitName.Length - 2), new UserHelper() { AppId = appId, Secret = secretId });
                    }
                }
            }
            catch(Exception ex)
            {
                _logger.LogTrace(ex.Message);
                throw new Exception(ex.Message);
            }


        }

        static IOrganizationService ConnectToMSCRM()
        {
            try
            {
                string connectionString = $"AuthType=ClientSecret; url={_organizationUrl};ClientId={_appId}; ClientSecret={_secretId}";
                CrmServiceClient.MaxConnectionTimeout = new TimeSpan(0, 15, 0);
                CrmServiceClient crmServiceClient = new CrmServiceClient(connectionString); //Connecting to the D-365 CE instance
                if (crmServiceClient != null && crmServiceClient.IsReady)
                    return crmServiceClient;
                else
                    throw new Exception("Could NOT connect to D365 CE instance.Please make sure the Connection String is correct.");
            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

       
    }
}
