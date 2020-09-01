using System.Linq;
using System;
using System.Collections.Specialized;

namespace Softwillians.ServiceBus.DeadLetterResender
{
    public static class Extensions
    {
        public static string GetOrThrow(this NameValueCollection appSettings, string key)
        {
            string _value = appSettings.Get(key);
            if (!appSettings.AllKeys.Contains(key) || _value == null || key.Equals(_value))
                throw new Exception($"Verifique o appsettings.json. Valor de {key} não foi definido corretamente");

            return _value;
        }
    }
}
