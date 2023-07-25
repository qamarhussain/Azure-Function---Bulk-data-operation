using Microsoft.Crm.Sdk.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NLR.SaleOrderCreation
{
    public class UserHelper
    {
        public string AppId { get; set; }
        public string Secret { get; set; }
        public string BusinessUnitName { get; set; }

    }

    public static class FunctionHelper
    {

        public static Dictionary<string, Guid> accountsKeyPairs = new Dictionary<string, Guid>();
        public static Dictionary<string, Guid> contactsKeyPairs = new Dictionary<string, Guid>();
        public static Dictionary<string, Guid> ordersKeyPairs = new Dictionary<string, Guid>();
        public static Dictionary<string, Guid> businessUnitsKeyPairs = new Dictionary<string, Guid>();

        public static Dictionary<string, UserHelper> businessUnitDictionary = new Dictionary<string, UserHelper>();

        //public static void CreateStaticData()
        //{
        //    businessUnitDictionary.Add("Milident Laboratoire Dentaire", new UserHelper() { AppId = "6ea6d8a5-5939-417f-bf40-dd8e8caa8ba8", Secret = "yCm8Q~lcny-xXFdTBIL1NCsNci4IMvWavSSFib29" });
        //    businessUnitDictionary.Add("Shanto Dental Ceramics", new UserHelper() { AppId = "8d83df98-8a8b-4e70-a55a-4a71e3818e6f", Secret = "3s28Q~eQbxfR~LXzxjRGmqYyrFVboCI4En_eAcKX" });
        //    businessUnitDictionary.Add("Centreline Dental Lab", new UserHelper() { AppId = "8e575be4-751d-417f-83c4-7c86acc02d8f", Secret = "bPv8Q~Mgx7hL4iGwJD4fHGANh56hzl_s9HInccOE" });
        //    businessUnitDictionary.Add("Digital One", new UserHelper() { AppId = "7ec95a20-c443-47af-bf45-e76ac514d61a", Secret = "6mN8Q~umk3eRFxAzk.uP5l7STKUNroiuK1wIdbyl" });
        //    businessUnitDictionary.Add("Digital One Dental Technologies Inc.", new UserHelper() { AppId = "96165606-ba36-4b26-bdfc-9c2d292453eb", Secret = "pfp8Q~NUieZ59zk16-e_u3PT96kTJIjT74fZRbPm" });
        //    businessUnitDictionary.Add("Pioneer Dental Laboratory", new UserHelper() { AppId = "22ead9c4-edd0-4d6f-bd18-fe57e995d516", Secret = "OLP8Q~GWiMNZVoBbTu~8wYeZTNhowYBTzQbIjbOs" });
        //    businessUnitDictionary.Add("Capital Ceramics", new UserHelper() { AppId = "3d402c1e-2909-426a-bc6b-4c2eb0c69874", Secret = "QIL8Q~IBgPDe1zJnSQKKczx2hsfpTYIIbizlPaZJ" });
        //    businessUnitDictionary.Add("Capital Ceramics Dental Laboratory", new UserHelper() { AppId = "d68472af-e5e6-480f-a478-e3c3caf1e5c1", Secret = "afv8Q~zMNOc5VZbiiyc9c7h-N6mnCQ1X9y3~Wa1Y" });
        //    businessUnitDictionary.Add("Shanto Dental Lab", new UserHelper() { AppId = "8febc3de-161a-47d0-9841-0428c9731931", Secret = "Wnx8Q~nLaHonl3BbsUZNtFC3OfGYty2vzz1vJa~H" });
        //}

        static UserHelper GetAppUserInfo(string businessUnitName, ILogger _logger)
        {
            try
            {
                return businessUnitDictionary.Where(x => x.Key == businessUnitName).Select(e => e.Value).First();
            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        static IOrganizationService ConnectToMSCRM(string appId, string secret, ILogger _logger, string _organizationUrl)
        {
            try
            {
                string connectionString = $"AuthType=ClientSecret; RequireNewInstance=True; url={_organizationUrl};ClientId={appId}; ClientSecret={secret}";
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



        public static bool CreateAccounts(IOrganizationService mainService, List<Account> accounts, ILogger _logger, List<IGrouping<string, Account>> groupedAccounts, string _organizationUrl)
        {
            try
            {
                bool accountStatus = true;

                // Getting business units Guid ids.
                GetBusinessUnitGuidIds(mainService, accounts.Select(x => x.sig_businessunitname).Distinct().ToList(), _logger);

                foreach(var grpAccount in groupedAccounts)
                {
                    var appUserInfo = GetAppUserInfo(grpAccount.Key, _logger);
                    if(appUserInfo != null)
                    {
                        _logger.LogInformation($"FUNC_SaleOrderBulkCreation - Business Unit: {grpAccount.Key}.  Total Accounts: {grpAccount.ToList().Count}");

                        var service = ConnectToMSCRM(appUserInfo.AppId, appUserInfo.Secret, _logger, _organizationUrl);

                    var groupedResult = from s in grpAccount.Select(x=>x)
                                        group s by s.accountid;
                        var groupedAccountsChunks = FunctionHelper.ChunkBy(groupedResult.ToList(), 999);
                        foreach(var chunk in groupedAccountsChunks)
                        {
                            var multipleRequest = new ExecuteMultipleRequest()
                            {
                                Settings = new ExecuteMultipleSettings()
                                {
                                    ContinueOnError = false,
                                    ReturnResponses = true
                                },
                                Requests = new OrganizationRequestCollection()
                            };

                            foreach (var item in chunk)
                            {

                                var res = item.First();

                                KeyAttributeCollection keyColl = new KeyAttributeCollection();
                                keyColl.Add("sig_accountidentifier", item.Key);

                                Entity accountEntity = new Entity("account", keyColl);
                                accountEntity["sig_accountidentifier"] = item.Key;
                                accountEntity["name"] = res.name;
                                accountEntity["address1_postalcode"] = res.PostalCode;
                                accountEntity["sig_corporatename"] = res.sig_corporatename;
                                accountEntity["sig_workflow"] = res.sig_workflow;
                                accountEntity["sig_groupname"] = res.sig_groupname;
                                accountEntity["overriddencreatedon"] = Convert.ToDateTime(res.overriddencreatedon.Substring(0, 10));

                                var businessUnitGuid = GetBusinessUnitId(res.sig_businessunitname, _logger);
                                if (!string.IsNullOrEmpty(Convert.ToString(businessUnitGuid)))
                                    accountEntity["sig_businessunit"] = new EntityReference("businessunit", businessUnitGuid);

                                UpsertRequest upsertRequest = new UpsertRequest { Target = accountEntity };
                                multipleRequest.Requests.Add(upsertRequest);
                            }


                            ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);

                            var result = multipleResponse.Results.Where(x => x.Key == "IsFaulted");

                            var responsStatus = !Convert.ToBoolean(result.ToList().First().Value);
                            _logger.LogInformation($"FUNC_SaleOrderBulkCreation - Accounts creation status: {responsStatus}");
                            if (responsStatus)
                            {
                                GetAccountGuidIds(mainService, groupedResult.Select(x => x.Key).ToList(), _logger);
                            }
                            accountStatus = responsStatus;

                            if (!responsStatus)
                                break;
                        }

                    }
                  
                }
                return accountStatus;


            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        public static bool CreateContacts(IOrganizationService mainService, List<Contact> contacts, ILogger _logger, List<IGrouping<string, Contact>> groupedContacts, string _organizationUrl)
        {
            try
            {
               
                bool contactStatus = true;

               foreach(var grpContact in groupedContacts)
                {
                    var appUserInfo = GetAppUserInfo(grpContact.Key, _logger);

                    if(appUserInfo != null)
                    {
                        _logger.LogInformation($"FUNC_SaleOrderBulkCreation - Business Unit: {grpContact.Key}.  Total Contacts: {grpContact.ToList().Count}");

                        var service = ConnectToMSCRM(appUserInfo.AppId, appUserInfo.Secret, _logger, _organizationUrl);

                        var groupedResult = from s in grpContact.Select(x=>x).ToList()
                                            group s by s.sig_contactidentifier;

                        var groupedContactChunks = FunctionHelper.ChunkBy(groupedResult.ToList(), 999);
                        foreach(var chunk in groupedContactChunks)
                        {
                            var multipleRequest = new ExecuteMultipleRequest()
                            {
                                Settings = new ExecuteMultipleSettings()
                                {
                                    ContinueOnError = false,
                                    ReturnResponses = true
                                },
                                Requests = new OrganizationRequestCollection()
                            };

                            foreach (var item in chunk)
                            {

                                KeyAttributeCollection keyColl = new KeyAttributeCollection();
                                keyColl.Add("sig_contactidentifier", item.Key);

                                var res = item.First();

                                Entity contactEntity = new Entity("contact", keyColl);
                                contactEntity["sig_contactidentifier"] = item.Key;
                                contactEntity["firstname"] = "";
                                contactEntity["lastname"] = res.fullname;
                                contactEntity["overriddencreatedon"] = Convert.ToDateTime(res.overriddencreatedon.Substring(0, 10));
                                contactEntity["parentcustomerid"] = new EntityReference("account", GetAccountId(res.accountid, _logger));

                                UpsertRequest upsertRequest = new UpsertRequest { Target = contactEntity };
                                multipleRequest.Requests.Add(upsertRequest);
                            }


                            ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);

                            var result = multipleResponse.Results.Where(x => x.Key == "IsFaulted");

                            var status = !Convert.ToBoolean(result.ToList().First().Value);
                            _logger.LogInformation($"FUNC_SaleOrderBulkCreation - Contacts creation status: {status}");

                            contactStatus = status;
                            if (!status)
                                break;
                        }

                    }

                }
                if (contactStatus)
                {
                    GetContactsGuidIds(mainService, contacts.Select(x => x.sig_contactidentifier).Distinct().ToList(), _logger);
                }
                return contactStatus;

            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        public static bool CreateOrders(IOrganizationService mainService, List<Order> orders, ILogger _logger, List<IGrouping<string, Order>> groupedOrders, string _organizationUrl)
        {
            try
            {
                bool orderCreateStatus = true;

                

                foreach(var grpOrders in groupedOrders)
                {
                    var appUserInfo = GetAppUserInfo(grpOrders.Key, _logger);
                    if(appUserInfo != null)
                    {
                        _logger.LogInformation($"FUNC_SaleOrderBulkCreation - Business Unit: {grpOrders.Key}.  Total Sales orders: {grpOrders.ToList().Count}.");

                        var service = ConnectToMSCRM(appUserInfo.AppId, appUserInfo.Secret, _logger, _organizationUrl);

                        var groupedResult = from s in grpOrders.Select(x=>x).ToList()
                                            group s by s.sig_saleorderid;

                        var groupedOrderChunks = FunctionHelper.ChunkBy(groupedResult.ToList(), 999);
                        foreach(var groupedChunk in groupedOrderChunks)
                        {
                            var multipleRequest = new ExecuteMultipleRequest()
                            {
                                Settings = new ExecuteMultipleSettings()
                                {
                                    ContinueOnError = false,
                                    ReturnResponses = true
                                },
                                Requests = new OrganizationRequestCollection()
                            };

                            foreach (var item in groupedChunk)
                            {
                                KeyAttributeCollection keyColl = new KeyAttributeCollection();
                                keyColl.Add("sig_saleorderid", item.Key);

                                var res = item.First();
                                Entity orderEntity = new Entity("salesorder", keyColl);
                                orderEntity["sig_saleorderid"] = item.Key;
                                orderEntity["sig_routename"] = res.sig_routename;
                                orderEntity["name"] = res.ordernumber;
                                orderEntity["pricelevelid"] = new EntityReference("pricelevel", Guid.Parse("A3844CE7-C5DC-49E2-8475-70460D6F104C"));
                                orderEntity["customerid"] = new EntityReference("contact", GetContactId(res.customerid, _logger));
                                var dateCreated = Convert.ToDateTime(res.overriddencreatedon.Split(',')[0]);
                                orderEntity["overriddencreatedon"] = dateCreated.ToUniversalTime();

                                UpsertRequest upsertRequest = new UpsertRequest { Target = orderEntity };
                                multipleRequest.Requests.Add(upsertRequest);
                            }


                            ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);

                            var result = multipleResponse.Results.Where(x => x.Key == "IsFaulted");

                            var status = !Convert.ToBoolean(result.ToList().First().Value);
                            _logger.LogInformation($"FUNC_SaleOrderBulkCreation - Orders creation status: {status}");

                            orderCreateStatus = status;

                            if (!status)
                                break;

                        }

                    }

                }
                if (orderCreateStatus)
                {
                    GetOrdersGuidIds(mainService, orders.Select(x => x.sig_saleorderid).Distinct().ToList(), _logger);
                }

                var ordersGuids = ordersKeyPairs.Select(x => x.Value).Distinct().ToList();
                DeleteOrderLines(ordersGuids, mainService, _logger);

                return orderCreateStatus;

            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        public static bool DeleteOrderLines(List<Guid> orderIds, IOrganizationService service, ILogger _logger)
        {
            try
            {
                var productChunks = FunctionHelper.ChunkBy(orderIds, 500);
                foreach(var chunk in productChunks)
                {
                    string cond = string.Empty;
                    foreach (var item in chunk)
                    {
                        cond = cond + "<value uiname='108976' uitype='salesorder'>" + item + @"</value>";
                    }

                    string fetchXml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
  <entity name='salesorderdetail'>
    <attribute name='salesorderdetailid' />
    <filter type='and'>
      <condition attribute='salesorderid' operator='in'>
       '" + cond + @"'
      </condition>
    </filter>
  </entity>
</fetch>";
                    EntityCollection orderLines = service.RetrieveMultiple(new FetchExpression(fetchXml));
                    if (orderLines.Entities.Any())
                    {
                        bool lineDeleteStatus = true;
                        var orderLinesChunks = FunctionHelper.ChunkBy(orderLines.Entities.ToList(), 999);
                        foreach(var lineChunk in orderLinesChunks)
                        {
                            var multipleRequest = new ExecuteMultipleRequest()
                            {
                                Settings = new ExecuteMultipleSettings()
                                {
                                    ContinueOnError = false,
                                    ReturnResponses = true
                                },
                                Requests = new OrganizationRequestCollection()
                            };

                            foreach (var item in lineChunk)
                            {
                                DeleteRequest deleteRequest = new DeleteRequest { Target = new EntityReference("salesorderdetail", item.GetAttributeValue<Guid>("salesorderdetailid")) };
                                multipleRequest.Requests.Add(deleteRequest);
                            }

                            ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);


                            var result = multipleResponse.Results.Where(x => x.Key == "IsFaulted");

                            var status = !Convert.ToBoolean(result.ToList().First().Value);

                            lineDeleteStatus = status;
                            if (!lineDeleteStatus)
                                break;
                        }

                        return lineDeleteStatus;

                    }
                }
              
                return true;
            }
            catch(Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        public static bool CreateOrderDetails(ILogger _logger, List<IGrouping<string, OrderDetail>> groupedOrderDetails, string _organizationUrl)
        {
            try
            {
                bool orderDetailStatus = false;

                foreach (var grpOrderDetails in groupedOrderDetails)
                {
                    _logger.LogInformation($"FUNC_SaleOrderBulkCreation - Business Unit: {grpOrderDetails.Key}.  Order details total: {grpOrderDetails.ToList().Count}.");
                    var appUserInfo = GetAppUserInfo(grpOrderDetails.Key, _logger);
                    if(appUserInfo != null)
                    {
                        var service = ConnectToMSCRM(appUserInfo.AppId, appUserInfo.Secret, _logger, _organizationUrl);

                        var groupedResult = grpOrderDetails.Select(x=>x).ToList()
        .GroupBy(x => new { x.SaleOrderIdentifier, x.productdescription })
        .Select(g => new
        {
            SaleOrderIdentifier = g.Key.SaleOrderIdentifier,
            ProductName = g.Key.productdescription,
            OrderDetail = g.ToList()
        });

                        if (groupedResult.Any())
                        {
                            var productChunks = FunctionHelper.ChunkBy(groupedResult.ToList(), 999);
                            foreach(var chunk in productChunks)
                            {
                                var multipleRequest = new ExecuteMultipleRequest()
                                {
                                    Settings = new ExecuteMultipleSettings()
                                    {
                                        ContinueOnError = false,
                                        ReturnResponses = true
                                    },
                                    Requests = new OrganizationRequestCollection()
                                };

                                foreach (var item in chunk)
                                {
                                    var saleOrderGuid = GetOrderId(item.SaleOrderIdentifier, _logger);
                                    KeyAttributeCollection keyColl = new KeyAttributeCollection();
                                    keyColl.Add("salesorderid", saleOrderGuid);
                                    keyColl.Add("sig_itemidentifier", item.ProductName);

                                    foreach (var eachItem in item.OrderDetail)
                                    {
                                        int qty = Convert.ToInt32(eachItem.quantity);
                                        var pricePerUnit = eachItem.baseamount / (qty == 0 ? 1 : qty);
                                        Entity orderDetailEntity = new Entity("salesorderdetail", keyColl);
                                        orderDetailEntity["sig_department"] = eachItem.sig_department;
                                        orderDetailEntity["quantity"] = eachItem.quantity;
                                        orderDetailEntity["priceperunit"] = new Money(pricePerUnit);
                                        orderDetailEntity["productdescription"] = eachItem.productdescription;
                                        orderDetailEntity["sig_producttype"] = eachItem.sig_producttype;
                                        orderDetailEntity["salesorderid"] = new EntityReference("salesorder", saleOrderGuid);

                                        UpsertRequest upsertRequest = new UpsertRequest { Target = orderDetailEntity };
                                        multipleRequest.Requests.Add(upsertRequest);
                                    }

                                }

                                ExecuteMultipleResponse multipleResponse = (ExecuteMultipleResponse)service.Execute(multipleRequest);


                                var result = multipleResponse.Results.Where(x => x.Key == "IsFaulted");

                                var status = !Convert.ToBoolean(result.ToList().First().Value);
                                _logger.LogInformation($"FUNC_SaleOrderBulkCreation - Order details creation status: {status}");

                                orderDetailStatus = status;
                                if (!status)
                                    break;
                            }
                          
                        }
                        else
                            orderDetailStatus = false;
                    }
 
                }

                return orderDetailStatus;
            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        static void GetBusinessUnitGuidIds(IOrganizationService service, List<string> businessUnitNames, ILogger _logger)
        {
            try
            {
                string condition = string.Empty;
                foreach (var item in businessUnitNames)
                {
                    condition = condition + $"<condition attribute='name' operator='eq' value='{item}' />";
                }

                string fetchxml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
  <entity name='businessunit'>
    <attribute name='name' />
    <attribute name='businessunitid' />
    <filter type='and'>
      <filter type='or'>
       " + condition + @"
      </filter>
    </filter>
  </entity>
</fetch>";

                EntityCollection ordersData = service.RetrieveMultiple(new FetchExpression(fetchxml));
                if (ordersData.Entities.Any())
                {
                    foreach (var item in ordersData.Entities)
                    {
                        businessUnitsKeyPairs.Add(Convert.ToString(item.GetAttributeValue<string>("name")), item.GetAttributeValue<Guid>("businessunitid"));
                    }
                }

            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        public static void GetAccountGuidIds(IOrganizationService service, List<string> accountsIdentifiers, ILogger _logger)
        {
            try
            {
                var accountsIdentifiersChunks = FunctionHelper.ChunkBy(accountsIdentifiers, 500);
                foreach(var chunk in accountsIdentifiersChunks)
                {
                    string condition = string.Empty;
                    foreach (var item in chunk)
                    {
                        condition = condition + $"<condition attribute='sig_accountidentifier' operator='eq' value='{item}' />";
                    }

                    string fetchXml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
  <entity name='account'>
    <attribute name='accountid' />
    <attribute name='sig_accountidentifier' />
    <attribute name='sig_accountid' />
    <filter type='and'>
      <filter type='or'>
       " + condition + @"
      </filter>
    </filter>
  </entity>
</fetch>";
                    EntityCollection accountsData = service.RetrieveMultiple(new FetchExpression(fetchXml));
                    if (accountsData.Entities.Any())
                    {
                        foreach (var item in accountsData.Entities)
                        {
                            accountsKeyPairs.Add(Convert.ToString(item.GetAttributeValue<string>("sig_accountidentifier")), item.GetAttributeValue<Guid>("accountid"));
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }

        }

        public static void GetContactsGuidIds(IOrganizationService service, List<string> contactsIdentifiers, ILogger _logger)
        {
            try
            {
                var contactsIdentifiersChunks = FunctionHelper.ChunkBy(contactsIdentifiers, 500);
                foreach(var chunk in contactsIdentifiersChunks)
                {
                    string condition = string.Empty;
                    foreach (var item in chunk)
                    {
                        condition = condition + $"<condition attribute='sig_contactidentifier' operator='eq' value='{item}' />";
                    }

                    string fetchxml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
  <entity name='contact'>
    <attribute name='contactid' />
 <attribute name='sig_contactidentifier' />
    <filter type='and'>
      <filter type='or'>
       " + condition + @"
      </filter>
    </filter>
  </entity>
</fetch>";

                    EntityCollection contactsData = service.RetrieveMultiple(new FetchExpression(fetchxml));
                    if (contactsData.Entities.Any())
                    {
                        foreach (var item in contactsData.Entities)
                        {
                            contactsKeyPairs.Add(Convert.ToString(item.GetAttributeValue<string>("sig_contactidentifier")), item.GetAttributeValue<Guid>("contactid"));
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        public static void GetOrdersGuidIds(IOrganizationService service, List<string> ordersIdentifiers, ILogger _logger)
        {
            try
            {

                var productChunks = FunctionHelper.ChunkBy(ordersIdentifiers, 500);
                foreach(var chunk in productChunks)
                {
                    string condition = string.Empty;
                    foreach (var item in chunk)
                    {
                        condition = condition + $"<condition attribute='sig_saleorderid' operator='eq' value='{item}' />";
                    }

                    string fetchxml = @"<fetch version='1.0' output-format='xml-platform' mapping='logical' distinct='false'>
  <entity name='salesorder'>
    <attribute name='salesorderid' />
    <attribute name='sig_saleorderid' />
    <filter type='and'>
      <filter type='or'>
        " + condition + @"
      </filter>
    </filter>
  </entity>
</fetch>";

                    EntityCollection ordersData = service.RetrieveMultiple(new FetchExpression(fetchxml));
                    if (ordersData.Entities.Any())
                    {
                        foreach (var item in ordersData.Entities)
                        {
                            ordersKeyPairs.Add(Convert.ToString(item.GetAttributeValue<string>("sig_saleorderid")), item.GetAttributeValue<Guid>("salesorderid"));
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        static List<List<T>> ChunkBy<T>(List<T> source, int chunkSize)
        {
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / chunkSize)
                .Select(x => x.Select(v => v.Value).ToList())
                .ToList();
        }

        public static Guid GetBusinessUnitId(string businessUnitName, ILogger _logger)
        {
            try
            {
                return businessUnitsKeyPairs.Where(x => x.Key == businessUnitName).Select(e => e.Value).First();
            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        public static Guid GetContactId(string contactIdentifier, ILogger _logger)
        {
            try
            {
                return contactsKeyPairs.Where(x => x.Key == contactIdentifier).Select(e => e.Value).First();
            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        static Guid GetAccountId(string accountIdentifier, ILogger _logger)
        {
            try
            {
                var acc = accountsKeyPairs.Where(x => x.Key == accountIdentifier).Select(e => e.Value).First();
                var accountId = Guid.Parse(acc.ToString().Replace("{", "").Replace("}", ""));
                return accountId;
            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }

        static Guid GetOrderId(string orderIdentifier, ILogger _logger)
        {
            try
            {
                return ordersKeyPairs.Where(x => x.Key == orderIdentifier).Select(e => e.Value).First();
            }
            catch (Exception ex)
            {
                _logger.LogError("FUNC_NLR_SaleOrderCreation - Exception - " + ex.Message);
                throw new Exception(ex.Message);
            }
        }


    }

    public class Account
    {
        public string AccountGuidId { get; set; }
        public string name { get; set; }
        public string PostalCode { get; set; }
        public string sig_businessunit { get; set; }
        public string accountid { get; set; }
        public string sig_corporatename { get; set; }
        public string sig_workflow { get; set; }
        public string sig_groupname { get; set; }
        public string overriddencreatedon { get; set; }
        public string sig_businessunitname { get; set; }

    }

    public class Contact
    {
        public string ContactGuidId { get; set; }
        public string fullname { get; set; }
        public string sig_contactidentifier { get; set; }
        public string sig_businessunit { get; set; }
        public string overriddencreatedon { get; set; }
        public string sig_businessunitname { get; set; }
        public string accountid { get; set; }
    }

    public class Order
    {
        public string sig_saleorderid { get; set; }
        public string ordernumber { get; set; }
        public string overriddencreatedon { get; set; }
        public string sig_routename { get; set; }
        public string pricelevelid { get; set; }
        public string customerid { get; set; }
        public string sig_businessunitname { get; set; }

    }

    public class OrderDetail
    {
        public string sig_department { get; set; }
        public decimal quantity { get; set; }
        public decimal baseamount { get; set; }
        public string sig_producttype { get; set; }

        public string productdescription { get; set; }  // In-line product name
        public string priceperunit { get; set; }

        public string salesorderid { get; set; }
        public string SaleOrderIdentifier { get; set; }
        public string sig_businessunitname { get; set; }


    }
}
