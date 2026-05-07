#!/usr/bin/env php
<?php

const NS = 'http://pear.php.net/dtd/package-2.1';

if (!isset($argv[1])) {
    fprintf(STDERR, "Missing version parameter\n");
    printUsage();
    exit(1);
}

$newVersion = $argv[1];

$doc = new DOMDocument();
if (!$doc->load(__DIR__.'/../package.xml')) {
    throw new \Exception('Failed loading package.xml');
}

$releaseNotes = generateReleaseNotes($newVersion);

moveCurrentReleaseToChangelog($doc);
updateCurrentRelease($doc, $newVersion, $releaseNotes);

file_put_contents(__DIR__.'/../package.xml', $doc->saveXML());

function printUsage(): void
{
    fprintf(STDERR, "Usage: %s <version>\n", $_SERVER['argv'][0]);
}

function generateReleaseNotes(string $newVersion): string
{
    $cmd = sprintf(
        'gh api repos/php-rdkafka/php-rdkafka/releases/generate-notes -f tag_name=%s -f target_commitish=%s',
        escapeshellcmd($newVersion),
        escapeshellcmd(exec("git rev-parse HEAD")),
    );

    $result = exec($cmd);
    if ($result === false) {
        throw new \Exception(sprintf('Command `%s` failed', $cmd));
    }

    $lines = explode("\n", json_decode($result, true)['body']);

    $newLines = [];
    $add = false;
    foreach ($lines as $line) {
        if (str_starts_with($line, '###')) {
            $add = true;
            $line = substr($line, 1);
        } elseif (str_starts_with($line, '##')) {
            $add = false;
        }
        if ($add) {
            $line = preg_replace(
                '# by (@[^ ]+) in https://github.com/[^ ]+/pull/([0-9]+)#',
                ' (#\2, \1)',
                $line,
            );
            $line = preg_replace(
                '#^\*#',
                '-',
                $line,
            );
            $newLines[] = $line;
        }
    }

    return implode("\n", $newLines);
}

function deriveApiVersion(string $version): string
{
    if (!preg_match('/^(\d+)\.(\d+)/', $version, $m)) {
        throw new \Exception("Cannot derive api version from $version");
    }
    return sprintf('%d.%d.0', $m[1], $m[2]);
}

function deriveStability(string $version): string
{
    if (preg_match('/(alpha|devel)/i', $version)) {
        return 'alpha';
    }
    if (preg_match('/(beta|RC)/i', $version)) {
        return 'beta';
    }
    return 'stable';
}

function updateCurrentRelease(DOMDocument $doc, string $newVersion, string $releaseNotes): void
{
    $date = date('Y-m-d');
    $time = date('H:i:s');

    $dateElem = findOneElement($doc, '/ns:package/ns:date');
    replaceContent($dateElem, $doc->createTextNode($date));

    $timeElem = findOneElement($doc, '/ns:package/ns:time');
    replaceContent($timeElem, $doc->createTextNode($time));

    $versionElem = findOneElement($doc, '/ns:package/ns:version/ns:release');
    replaceContent($versionElem, $doc->createTextNode($newVersion));

    $apiElem = findOneElement($doc, '/ns:package/ns:version/ns:api');
    replaceContent($apiElem, $doc->createTextNode(deriveApiVersion($newVersion)));

    $stability = deriveStability($newVersion);
    $stabilityReleaseElem = findOneElement($doc, '/ns:package/ns:stability/ns:release');
    replaceContent($stabilityReleaseElem, $doc->createTextNode($stability));
    $stabilityApiElem = findOneElement($doc, '/ns:package/ns:stability/ns:api');
    replaceContent($stabilityApiElem, $doc->createTextNode($stability));

    $notesElem = findOneElement($doc, '/ns:package/ns:notes');
    $releaseNotes = rtrim("\n  " . str_replace("\n", "\n  ", $releaseNotes))."\n ";
    replaceContent($notesElem, $doc->createTextNode($releaseNotes));
}

function moveCurrentReleaseToChangelog(DOMDocument $doc): void
{
    $oldRelease = $doc->createElementNS(NS, 'release');
    $oldRelease->appendChild($doc->createTextNode("\n"));

    $nodesToCopy = ['date', 'time', 'version', 'stability', 'license', 'notes'];

    foreach ($nodesToCopy as $nodeName) {
        $path = sprintf('/ns:package/ns:%s', $nodeName);
        $elem = findOneElement($doc, $path);

        $elem = $elem->cloneNode(true);

        $oldRelease->appendChild($doc->createTextNode(" "));
        $oldRelease->appendChild($elem);
        $oldRelease->appendChild($doc->createTextNode("\n"));
    }

    indent($oldRelease, '  ');

    $changelogElem = findOneElement($doc, '/ns:package/ns:changelog');
    $changelogElem->insertBefore($oldRelease, $changelogElem->firstChild);
    $changelogElem->insertBefore($doc->createTextNode("\n  "), $oldRelease);
}

function findOneElement(DOMDocument $doc, string $path): DOMElement
{
    $xp = new DOMXPath($doc, false);
    $xp->registerNamespace('ns', NS);

    $list = $xp->evaluate($path);
    if ($list->length !== 1) {
        throw new \Exception(sprintf(
            'XPath expression %s expected to find 1 element, but found %d',
            $path,
            $list->length,
        ));
    }

    return $list->item(0);
}

function indent(DOMElement $elem, string $indentString): void
{
    foreach ($elem->childNodes as $node) {
        if ($node instanceof DOMText) {
            $node->textContent = str_replace("\n", "\n".$indentString, $node->textContent);
        } else if ($node instanceof DOMElement) {
            indent($node, $indentString);
        }
    }
}

function replaceContent(DOMElement $elem, DOMNode $newContent): void
{
    while ($elem->firstChild !== null) {
        $elem->removeChild($elem->firstChild);
    }

    $elem->appendChild($newContent);
}

