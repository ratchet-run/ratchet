import { access, readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const websiteRoot = fileURLToPath(new URL('../', import.meta.url))
const repoRoot = resolve(websiteRoot, '..')
const docsRoot = resolve(websiteRoot, 'docs')

async function readDoc(relativePath) {
  return readFile(resolve(docsRoot, relativePath), 'utf8')
}

function requireText(relativePath, content, expected) {
  if (!content.includes(expected)) {
    throw new Error(`${relativePath} must contain: ${expected}`)
  }
}

function forbidText(relativePath, content, forbidden) {
  if (content.toLowerCase().includes(forbidden.toLowerCase())) {
    throw new Error(`${relativePath} must not contain: ${forbidden}`)
  }
}

async function requireDocLink(relativePath, content, href, targetHeading) {
  requireText(relativePath, content, `](${href})`)

  const [target, fragment] = href.split('#', 2)
  const targetPath = resolve(docsRoot, dirname(relativePath), target)
  await access(targetPath)

  if (fragment) {
    const targetContent = await readFile(targetPath, 'utf8')
    requireText(target, targetContent, targetHeading)
  }
}

const clusteringPath = 'concepts/clustering.md'
const executionModelPath = 'concepts/execution-model.md'
const clusterConfigurationPath = 'deployment/cluster-configuration.md'
const kubernetesPath = 'deployment/kubernetes.md'

const clustering = await readDoc(clusteringPath)
const executionModel = await readDoc(executionModelPath)
const clusterConfiguration = await readDoc(clusterConfigurationPath)
const kubernetes = await readDoc(kubernetesPath)
const riModuleInfo = await readFile(resolve(repoRoot, 'ratchet/src/main/java/module-info.java'), 'utf8')
const drainController = await readFile(
  resolve(repoRoot, 'ratchet/src/main/java/run/ratchet/ri/core/DrainController.java'),
  'utf8',
)

const staleClaims = [
  "readiness probes can check the drain controller's status",
  'Already-running jobs are allowed to finish within a timeout',
  'terminationGracePeriodSeconds` to allow in-flight jobs to finish',
]

for (const staleClaim of staleClaims) {
  for (const [path, content] of [
    [clusteringPath, clustering],
    [executionModelPath, executionModel],
    [clusterConfigurationPath, clusterConfiguration],
    [kubernetesPath, kubernetes],
  ]) {
    forbidText(path, content, staleClaim)
  }
}

forbidText('ratchet/src/main/java/module-info.java', riModuleInfo, 'exports run.ratchet.ri.core;')
requireText(
  'ratchet/src/main/java/run/ratchet/ri/core/DrainController.java',
  drainController,
  'Applications must not implement this interface',
)

requireText(
  clusteringPath,
  clustering,
  "`DrainController` is an internal implementation detail, not a public health-check API",
)
requireText(
  executionModelPath,
  executionModel,
  "`DrainController` is not a public health or lifecycle API",
)
requireText(kubernetesPath, kubernetes, '## Rolling termination')
requireText(kubernetesPath, kubernetes, 'preStop:')
requireText(kubernetesPath, kubernetes, 'terminationGracePeriodSeconds:')
requireText(kubernetesPath, kubernetes, '/tmp/ratchet-not-ready')
requireText(
  kubernetesPath,
  kubernetes,
  "does not switch Ratchet's internal drain mode",
)

await requireDocLink(
  clusteringPath,
  clustering,
  '../deployment/kubernetes.md#rolling-termination',
  '## Rolling termination',
)
await requireDocLink(
  executionModelPath,
  executionModel,
  '../deployment/kubernetes.md#rolling-termination',
  '## Rolling termination',
)
await requireDocLink(
  clusterConfigurationPath,
  clusterConfiguration,
  './kubernetes.md#rolling-termination',
  '## Rolling termination',
)

console.log('Documentation contracts passed')
