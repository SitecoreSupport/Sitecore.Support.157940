using Sitecore.Caching;
using Sitecore.Data;
using Sitecore.Diagnostics;
using System;
using System.Linq;
using Sitecore.Data.DataProviders;
using Sitecore.Collections;
using Sitecore.Data.Items;

namespace Sitecore.Support.Data.SqlServer
{
  class SqlServerDataProvider : Sitecore.Data.SqlServer.SqlServerDataProvider
  {
    public SqlServerDataProvider(string connectionString) : base(connectionString)
    {
    }

    [CanBeNull]
    public override ID ResolvePath([NotNull] string itemPath, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(itemPath, "itemPath");
      Assert.ArgumentNotNull(context, "context");

      if (ID.IsID(itemPath))
      {
        return new ID(itemPath);
      }

      return this.ResolvePathRec(
        itemPath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries),
        CacheManager.GetPathCache(context.DataManager.Database),
        context);
    }

    [CanBeNull]
    private ID ResolvePathRec([NotNull] string[] path, [NotNull] PathCache cache, [NotNull] CallContext context)
    {
      Assert.ArgumentNotNull(path, "path");
      Assert.ArgumentNotNull(cache, "cache");
      Assert.ArgumentNotNull(context, "context");

      if (path.Length == 0)
      {
        return null;
      }

      string itemPath = "/" + string.Join("/", path);
      ID id = cache.GetMapping(itemPath);
      if (id != (ID)null)
      {
        return id;
      }

      if (path.Length == 1)
      {
        ItemDefinition rootItem = this.GetItemDefinition(this.GetRootID(context), context);
        if (rootItem.Name.Equals(path[0], StringComparison.InvariantCultureIgnoreCase))
        {
          cache.AddMapping(itemPath, rootItem.ID);
          return rootItem.ID;
        }

        if (ID.IsID(path[0]))
        {
          var rootid = new ID(path[0]);
          if (rootid == rootItem.ID)
          {
            cache.AddMapping(itemPath, rootItem.ID);
            return rootid;
          }
        }

        return null;
      }

      var parentPath = new string[path.Length - 1];
      Array.Copy(path, parentPath, path.Length - 1);
      ID parentId = this.ResolvePathRec(parentPath, cache, context);
      if (parentId == (ID)null)
      {
        return null;
      }

      ItemDefinition parent = this.GetItemDefinition(parentId, context);
      if (parent == null)
      {
        // Fix start:
        // cache.RemoveMappingsContaining("/" + string.Join("/", parentPath));
        Log.Debug($"[Sitecore.Support.157940] Calling method 'PathCache.RemoveMappingsContaining' has been skipped.");
        // Fix ends.

        return null;
      }

      string itemName = path[path.Length - 1];
      if (ID.IsID(itemName))
      {
        var res = new ID(itemName);
        cache.AddMapping(itemPath, res);
        return res;
      }

      IdList childIDs = this.GetChildIdsByName(itemName, parentId);
      if (childIDs.Count == 0)
      {
        return null;
      }

      if (childIDs.Count == 1)
      {
        ID rslt = childIDs[0];
        cache.AddMapping(itemPath, rslt);
        return rslt;
      }

      Database database = context.DataManager.Database;
      Item parentItem = database.GetItem(parentId);
      Assert.IsNotNull(parentItem, "Item not found. ID: " + parentId + ", Path: " + parentPath);
      var itemList = new ItemList();
      itemList.AddRange(childIDs.Select(database.GetItem));
      var childList = new ChildList(parentItem, itemList);
      childList.Sort();

      ID result = childList[0].ID;
      cache.AddMapping(itemPath, result);
      return result;
    }
  }
}