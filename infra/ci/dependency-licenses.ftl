<#-- Machine-readable effective POM metadata. No remote license lookup. -->
{
  "dependencies": [
<#list dependencyMap as entry>
  <#assign project = entry.getKey()/>
    {
      "coordinate": "${project.groupId?json_string}:${project.artifactId?json_string}:${project.version?json_string}",
      "licenses": [<#list project.licenses as license>{"name": "${(license.name!"")?json_string}", "url": "${(license.url!"")?json_string}"}<#sep>, </#list>]
    }<#sep>,</#list>
  ]
}
