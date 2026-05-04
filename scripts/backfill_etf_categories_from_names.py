from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, ascii_fold, normalize_sector


DEFAULT_OUTPUT_DIR = ROOT / "data" / "etf_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "name_category_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "name_category_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "category_update",
    "matched_rule",
    "decision",
]
METADATA_UPDATE_FIELDNAMES = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
CLASSIFIER_METADATA_FIELDS = {"sector", "etf_category"}
CLASSIFIER_REASON_PREFIX = "Deterministic ETF-name classifier mapped"


@dataclass(frozen=True)
class CategoryRule:
    category: str
    name: str
    patterns: tuple[re.Pattern[str], ...]


def rule(category: str, name: str, *patterns: str) -> CategoryRule:
    normalized = normalize_sector(category, "ETF")
    if not normalized:
        raise ValueError(f"Unknown ETF category: {category}")
    return CategoryRule(
        category=normalized,
        name=name,
        patterns=tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns),
    )


ETF_CATEGORY_RULES: tuple[CategoryRule, ...] = (
    rule(
        "Money Market",
        "money_market",
        r"\bmoney market\b",
        r"\bcash\b",
        r"\bhigh interest savings\b",
        r"\bsavings account fund\b",
        "\uba38\ub2c8\ub9c8\ucf13",
    ),
    rule("Money Market", "money_market_asia", "\ud654\ud3d0", "\u8d27\u5e01"),
    rule(
        "Leveraged/Inverse",
        "leveraged_inverse",
        r"\bleveraged\b",
        r"\bleverage\b",
        r"\binverse\b",
        r"\bbull\b",
        r"\bbear\b",
        r"\b[23]x\b",
        r"\b\d+(?:\.\d+)?x long\b",
        r"\blong [a-z0-9 .&()-]+ daily etf\b",
        r"\bgeared\b",
        r"\blev\b",
        r"\bshort (?!term\b)",
        r"\bshortdax\b",
        r"\bwig20short\b",
        "\ub808\ubc84\ub9ac\uc9c0",
        "\uc778\ubc84\uc2a4",
    ),
    rule(
        "Alternative",
        "digital_assets",
        r"\bbitcoin\b",
        r"\bethereum\b",
        r"\bether\b",
        r"\bsolana\b",
        r"\bxrp\b",
        r"\bcardano\b",
        r"\bdogecoin\b",
        r"\bdoge\b",
        r"\blitecoin\b",
        r"\baltcoins?\b",
        r"\bpolkadot\b",
        r"\bchainlink\b",
        r"\bhedera\b",
        r"\bcelestia\b",
        r"\barbitrum\b",
        r"\binjective\b",
        r"\bstaking\b",
        r"\bstablecoins?\b",
        r"\btokeni[sz]ation\b",
        r"\bcrypto(?:currency)?\b",
        r"\bdigital assets?\b",
    ),
    rule(
        "Alternative",
        "alternative",
        r"\boption income\b",
        r"\boptions? etp\b",
        r"\byield premium strategy\b",
        r"\byield maximiser\b",
        r"\bhighshares\b",
        r"\b(?:tracker|tracking) (?:et[cp]|securities?)\b",
        r"\b1x [a-z0-9 .&()-]+ (?:tracker|et[cp])\b",
        r"\bpremium income\b",
        r"\bweeklypay\b",
        r"\byield(?:max|boost)\b",
        r"\bautocallable\b",
        r"\bbarrier\b",
        r"\bstructured outcome\b",
        r"\bmanaged futures?\b",
        r"\bmacro\b",
        r"\btactical\b",
        r"\babsolute return\b",
        r"\balternative\b",
        r"\ball weather\b",
        r"\ball[- ]weather\b",
        r"\btrend\b",
        r"\boption strategy\b",
        r"\bincomemax option\b",
        r"\bbox etf\b",
        r"\bsystematic alternatives\b",
        r"\bmarket neutral\b",
        r"\bhedged multi-asset\b",
        r"\basset allocation\b",
        r"\bbalanced(?:\\+)?(?: asset allocation| etf| fund| portfolio)?\b",
        r"\bconservative asset allocation\b",
        r"\bmoderate allocation\b",
        r"\baggressive allocation\b",
        r"\bhedged (?:multi-asset|portfolio|allocation)\b",
        r"\bstructured alt protection\b",
        r"\btarget 15\b",
        r"\bcovered call\b",
        r"\bbuffer\b",
        r"\bbuffered\b",
        r"\bcollared\b",
        r"\btail risk\b",
        r"\bbuywrite\b",
        r"\barbitrage\b",
        r"\bdeflation\b",
        r"\bstable income\b",
        r"\bincome bucket\b",
        r"\bdefined risk\b",
        r"\b30/70 conservative allocation\b",
        r"\bbattleshares\b",
        r"\bvs [a-z0-9 .&()-]+ etf\b",
        "\ucee4\ubc84\ub4dc\ucf5c",
    ),
    rule(
        "Multi-Asset",
        "multi_asset",
        r"\bmulti[- ]?asset\b",
        r"\ball[- ]in[- ]one conservative\b",
        r"\bconservative income\b",
        r"\bconservative etf\b",
        r"\btarget leading sector moderate\b",
        r"\bmindful conservative\b",
        r"\bmoderate etf\b",
        r"\breal assets fund\b",
        r"\breal assets\b",
    ),
    rule("Corporate Bonds", "corporate_bonds", r"\bcorporate bonds?\b", r"\bcorp(?:orate)?\b", r"\bcorp ?etf\b", "\ud68c\uc0ac\ucc44"),
    rule("Treasury Bonds", "treasury_bonds", r"\btreasur(?:y|ies)\b", r"\bt-?bills?\b", r"\bktb\b", r"\bt-?note\b", "\uad6d\ucc44"),
    rule("High Yield Bonds", "high_yield_bonds", r"\bhigh yield\b", r"\bjunk bonds?\b"),
    rule("Inflation-Protected Securities", "inflation_protected", r"\btips\b", r"\binflation[- ]protected\b"),
    rule(
        "Investment Grade Bonds",
        "investment_grade_bonds",
        r"\binvestment grade\b",
        r"\bhigh grade\b",
        r"\big bonds?\b",
        r"\baaa[- ]?a\b",
    ),
    rule(
        "Fixed Income",
        "fixed_income",
        r"\bfixed income\b",
        r"\bbonds?\b",
        r"\bcredit\b",
        r"\bclo\b",
        r"\bcmbs\b",
        r"\bmbs\b",
        r"\bsenior loan\b",
        r"\bloan\b",
        r"\bfloating rate\b",
        r"\bincome fund\b",
        r"\bstable income\b",
        r"\bincome plus\b",
        r"\bglobal income\b",
        r"\bmonthly income\b",
        r"\bdiversified monthly income\b",
        r"\bincome optimiser\b",
        r"\bpreferred securit(?:y|ies)\b",
        r"\bpreferred securities? & inc\b",
        r"\bdebt\b",
        r"\bmortgage\b",
        r"\brmbs\b",
        r"\basset finance\b",
        r"\bseries \d{4}-\d+\b",
        r"\b\d{4}-\d+(?:nc)? trust\b",
        r"\babs\b",
        r"\btrust (?:no\.)?\s*\d{4}",
        r"\btrust \d{4}-\d+\b",
        r"\bdriver australia\b",
        r"\breds trust\b",
        r"\bfidc\b",
        r"\bdireitos credit[oó]rios\b",
        r"\binc de inv inf rf\b",
        r"\bmulti-sector income\b",
        r"\bhigh income\b",
        r"\bhigh[- ]yield\b",
        r"\bsecuritized income\b",
        r"\bgovernment securities\b",
        r"\bintermediate government\b",
        r"\bincome opportunities\b",
        r"\bdiversified income\b",
        r"\bactive income\b",
        r"\bstrategic income\b",
        r"\btargeted income\b",
        r"\bdeferred income\b",
        r"\bdurable income\b",
        r"\blongevity income\b",
        r"\bactive yield\b",
        r"\bbrookstone yield\b",
        r"\binterest rate\b",
        r"\bdefined duration\b",
        r"\bterm income\b",
        r"\bspecialty lending\b",
        r"\bobligacji\b",
        r"\btbsp\b",
        r"\babsolute return bnd\b",
        r"\bhybrid opportunities\b",
        r"\bopportunistic income\b",
        r"\bflexible income\b",
        r"\bcore plus\b",
        r"\btotal return\b",
        r"\brenda fixa\b",
        r"\bcredito privado\b",
        r"\bcdi\b",
        r"\bdap\d+\b",
        r"\bima-b\b",
        r"\binfra\b",
        r"\brenta fija\b",
        r"\bcorta duracion\b",
        r"\bwgbi\b",
        r"\bmunicipal\b",
        r"\bmuni\b",
        "\ucc44\uad8c",
        "\u503a",
    ),
    rule("Volatility", "volatility", r"\bvolatility\b", r"\bdefined volatility\b"),
    rule(
        "Commodities Broad Basket",
        "commodities",
        r"\bcommodit(?:y|ies)\b",
        r"\bcarbon allowance\b",
        r"\bgold\b",
        r"\bsilver\b",
        r"\bcopper\b",
        r"\bsugar\b",
        r"\boil\b",
        r"\bcrude oil\b",
        r"\bnatural gas\b",
        r"\buranium\b",
        r"\blivestock\b",
        r"\bagriculture\b",
        r"\bsoybeans?\b",
        r"\bpalladium\b",
        r"\baluminium\b",
        r"\bbrent\b",
        r"\bwti\b",
        r"\bboi gordo\b",
        "\uc6d0\uc720",
        "\ucc9c\uc5f0\uac00\uc2a4",
        "\u9ec4\u91d1",
        "\u8c46",
    ),
    rule("Currencies", "currencies", r"\bcurrency(?: basket| etf| fund|shares| strategy)\b", r"\bforex\b", r"\bfx\b", r"\bdollar index\b", r"\busd futures\b", r"\bus dollar\b", r"\baustralian dollar\b", r"\bjpykrw futures\b"),
    rule(
        "Real Estate",
        "brazil_real_estate_funds",
        r"\bfii\b",
        r"\bfdo (?:de )?inv imob\b",
        r"\bfdo .* imob\b",
        r"\bfiagro\b",
        r"\bfundo de investimento imob\b",
        r"\breceb[ií]veis imob\b",
        r"\brenda imob",
        r"\bshopping fundo\b",
    ),
    rule("Small Cap", "small_cap", r"\bsmall[- ]?cap\b"),
    rule("Mid Cap", "mid_cap", r"\bmid[- ]?cap\b", r"\bmid[- ]small\b", "\ubbf8\ub4dc\ucea1100"),
    rule(
        "Large Cap",
        "large_cap",
        r"\blarge[- ]?cap\b",
        r"\bs&p ?500\b",
        r"\bnasdaq ?100\b",
        r"\bnasdaq[- ]?100\b",
        r"\bdow jones(?: industrial average)?\b",
        r"\bdow industrial\b",
        r"\bdjia\b",
        r"\bftse ?100\b",
        r"\bnikkei ?225\b",
        r"\btopix\b",
        r"\bjpx\b",
        r"\bnifty ?50\b",
        r"\bnifty\b",
        r"\ba100\b",
        r"\btopix ?100\b",
        r"\bkrx ?100\b",
        r"\bkospi\b",
        r"\bkosdaq ?150\b",
        r"\bchina h\b",
        r"\bhscei\b",
        r"\bdjia\b",
        r"\bs&p ?100\b",
        r"\btiger 200\b",
        r"\bkodex 200\b",
        r"\bkospi 200\b",
        r"\bcsi ?(?:300|500|800|a500)\b",
        r"\bsse ?(?:50|180|380)\b",
        r"\bchina ?50\b",
        r"\bhang seng\b",
        r"\btai(?:ex|wan 50)\b",
        r"\btop ?(?:10|20|30|50|100)\b",
        r"\bnyse fang\\+\b",
        r"\bfang\+",
        r"\bnyse us 50\b",
        r"\bus 50 etf\b",
        r"\baustralia 300\b",
        r"\bszse ?100\b",
        r"\bszse sme\b",
        r"\bus ?100 etf\b",
        r"\bnasdaq next gen 100\b",
        r"\bfounders 100\b",
        r"\bglobal 100\b",
        r"\brussell 2000\b",
        r"\bwig20\b",
        r"\bmwig40\b",
        r"\bswig80\b",
        r"\btaiwan (?:50|future 50|smart select|select premium|excellence|harvest)\b",
        r"\bjapan global moat\b",
        r"\bglobal industry elite\b",
        r"\beuro stoxx ?50\b",
        r"\bstoxx global\b",
        r"\bglobal x .* top\b",
        r"\bshenzhen ?50\b",
        r"\ba50\b",
        "\u4e0a\u8bc150",
        "\u6df1\u8bc150",
        "\u6caa\u6df1300",
        "\u7eb3\u65af\u8fbe\u514b100",
        "\u7eb3\u6307",
        "\u6807\u666e500",
        "\u521b\u4e1a\u677f200",
        "\u521b\u4e1a\u677f\u7efc",
        "\ub098\uc2a4\ub2e5 ?100",
        "\ubbf8\uad6ds&p500",
    ),
    rule("Growth", "growth", r"\bgrowth\b"),
    rule("Value", "value", r"\bvalue\b", r"\bdividend\b", r"\bdywidenda\b", r"\bsuperdividend\b", "\ubc30\ub2f9", "\u7ea2\u5229", "\u4f4e\u6ce2"),
    rule(
        "Factors",
        "factors",
        r"\bfactor\b",
        r"\bquality\b",
        r"\bmomentum\b",
        r"\blow volatility\b",
        r"\blow[- ]vol\b",
        r"\bequal weight\b",
        r"\bequalweight\b",
        r"\besg\b",
        r"\bsri\b",
        r"\bmultifactor\b",
        r"\benhanced index\b",
        r"\bscreened\b",
        r"\benhanced\b",
        r"\bfundamental\b",
        r"\bsocial resp\b",
        "\u53ef\u6301\u7eed\u53d1\u5c55",
        "\u8d28\u91cf",
    ),
    rule("Emerging Markets", "emerging_markets", r"\bemerging markets?\b", r"\bmsci em\b", r"\blatin\b"),
    rule("Developed Markets", "developed_markets", r"\bdeveloped markets?\b", r"\bmsci world\b", r"\bmsci eafe\b", r"\bdax\b", "\u6cd5\u56fd", "\u4e1c\u8bc1", "\u65e5\u7ecf"),
    rule("Health Care", "health_care", r"\bhealth ?care\b", r"\bmedical\b", r"\bmedical devices?\b", r"\bbiotech\b", r"\bpharma(?:ceuticals?)?\b", r"\bbio\b", r"\boncology\b", r"\bgenomic\b", r"\bgenomics\b", r"\bvaccine\b", "\uc758\ub8cc", "\ubc14\uc774\uc624", "\u533b\u7597", "\u521b\u65b0\u836f", "\u75ab\u82d7", "\u4e2d\u836f", "\u533b\u836f", "\u751f\u7269\u533b\u836f", "\u751f\u7269\u79d1\u6280"),
    rule("Information Technology", "information_technology", r"\btechnology\b", r"\btechnologies\b", r"\btechnical lead\b", r"\btchnlgy\b", r"\bdigital transformation\b", r"\bdigital innovation\b", r"\bdigital payment\b", r"\be-?commerce\b", r"\bdisruptive automation\b", r"\bdisruptive finance\b", r"\bdisruptors?\b", r"\btransform systems\b", r"\bwafer\b", r"\bsemiconductor\b", r"\bsemicon\b", r"\bsoftware\b", r"\bai\b", r"\bquantum computing\b", r"\bcloud(?: computing)?\b", r"\bcyber(?:security)?\b", r"\bcomputer\b", r"\b5g\b", r"\bfintech\b", r"\bcleantech\b", r"\bbig data\b", r"\bstreaming\b", r"\bgaming\b", r"\bvideo games?\b", r"\besports?\b", r"\bcritical technologies\b", r"\bblockchain\b", r"\bweb3\b", r"\brobot", r"\bdrone\b", r"\bdrones\b", r"\binformation security\b", r"\biot\b", "\ubc18\ub3c4\uccb4", "\ud14c\ud06c", "\uc591\uc790\ucef4\ud4e8\ud305", "\uc815\ubcf4\ubcf4\uc548", "\uc2e0\ucc3d", "\u79d1\u6280", "\u5927\u6570\u636e", "\u901a\u4fe1", "\u7269\u8054\u7f51", "\u4eba\u5de5\u667a\u80fd", "\u6570\u5b57\u7ecf\u6d4e", "\u534a\u5bfc\u4f53", "\u96c6\u6210\u7535\u8def", "\u82af\u7247", "\u4e91\u8ba1\u7b97", "\u4fe1\u521b", "\u7f51\u7edc\u5b89\u5168", "\u901a\u4fe1"),
    rule("Financials", "financials", r"\bfinancials?\b", r"\bbanks?\b", r"\binsurance\b", "\uc740\ud589", "\ubcf4\ud5d8", "\uc99d\uad8c", "\u94f6\u884c", "\u91d1\u878d", "\u8bc1\u5238"),
    rule("Energy", "energy", r"\benergy\b", r"\boil\b", r"\bgas\b", r"\buranium\b", r"\bmlp\b", r"\bcoal\b", "\u77f3\u6cb9", "\u77f3\u5316", "\u65b0\u80fd\u6e90\u8f66", "\u5149\u4f0f"),
    rule("Real Estate", "real_estate", r"\breit\b", r"\breits\b", r"\breal estate\b", r"\bproperty trust\b", r"\bproperty fund\b", r"\bprop sec\b", r"\bproperties\b", r"\bbuilding fund\b", r"\bmetropolitan fund\b", r"\bimob(?:ili[áa]rio)?\b", r"\blog[ií]stic", r"\bshoppings?\b", r"\bresidenciais\b", r"\burbana\b", "\u5730\u4ea7", "\u623f\u5730\u4ea7"),
    rule("Utilities", "utilities", r"\butilities\b", r"\bpower\b", "\u7535\u529b", "\uc804\ub825"),
    rule("Materials", "materials", r"\bmaterials\b", r"\bmining\b", r"\bmetals?\b", r"\blithium\b", r"\bbattery tech\b", r"\bnatural resources\b", r"\bnon-ferrous\b", "\ud76c\uc18c\uae08\uc18d", "\u5316\u5de5", "\u5efa\u6750", "\u7a00\u571f", "\u7a00\u6709\u91d1\u5c5e", "\u65b0\u6750\u6599", "\u6709\u8272"),
    rule("Consumer Staples", "consumer_staples", r"\bconsumer staples\b", r"\bfood\b", r"\bwine\b", r"\bagri\b", "\u519c\u4e1a", "\u519c\u7267", "\u98df\u54c1\u996e\u6599", "\u7cae\u98df", "\u517b\u6b96"),
    rule("Consumer Discretionary", "consumer_discretionary", r"\bconsumer discretionary\b", r"\bdiscretionary spending\b", r"\bconsumer trends?\b", r"\bhome construction\b", r"\bcosmetics\b", r"\btour(?:ism)?\b", r"\bleisure\b", r"\bautos?\b", "\u5316\u5986\u54c1", "\u5bb6\u7535", "\u6559\u80b2", "\u65c5\u6e38", "\u6c7d\u8f66", "\u6d88\u8d39", "\uc18c\ube44"),
    rule("Communication Services", "communication_services", r"\bcommunication services\b", r"\bcommunications?\b", r"\btelecommunications?\b", r"\bmedia\b", r"\btelecom\b", r"\binternet\b", "\u4e2d\u6982\u4e92\u8054", "\u4e2d\u6982\u4e92\u8054\u7f51", "\u4e92\u8054\u7f51", "\u4f20\u5a92", "\u6e38\u620f", "\ud1b5\uc2e0"),
    rule("Industrials", "industrials", r"\bindustrials?\b", r"\baerospace\b", r"\bsatellite\b", r"\bshipping\b", r"\bmanufactur(?:ing|ers?)\b", r"\binfrastructure\b", r"\binfr\b", r"\bconstruction\b", r"\bheavy industry\b", r"\bdefense\b", r"\bmilitary\b", r"\bautonomous\b", r"\belectric vehicles?\b", r"\bhumanoid\b", r"\bmachinar(?:y|ies)\b", r"\bequipment\b", "\u4e00\u5e26\u4e00\u8def", "\u519b\u5de5", "\u519b\u5de5\u9f99\u5934", "\u4ea4\u8fd0", "\u4ea4\u901a\u8fd0\u8f93", "\u5de5\u4e1a\u6bcd\u673a", "\u673a\u5e8a", "\u673a\u5668\u4eba", "\u539f\u5b50\u529b", "\u56fd\u9632", "\u539f\u5b50\u529b", "\u6c7d\u8f66", "\u9ad8\u7aef\u88c5\u5907", "\ub85c\ubd07", "\ud734\uba38\ub178\uc774\ub4dc", "\uc6d0\uc790\ub825", "\uc81c\uc870", "\u57fa\u5efa", "\u5236\u9020"),
    rule(
        "Equities",
        "equities",
        r"\bequit(?:y|ies)\b",
        r"\bstocks?\b",
        r"\bshares?\b",
        r"\bindex fund\b",
        r"\bucits etf\b",
        r"\bexchange traded fund\b",
        r"\bindex etf\b",
        r"\bidx etf\b",
        r"\bfund(?:o)? de [ií]ndice\b",
        r"\bfund de [ií]ndice\b",
        r"\bfdo ind\b",
        r"\btechnology and entrepreneurship\b",
        r"\binnovative transaction\b",
        r"\bmetaverse\b",
        r"\bblockchain\b",
        r"\bai\b",
        r"\bartificial intelligence\b",
        r"\bsemiconductor\b",
        r"\bmsci\b",
        r"\bftse\b",
        r"\bsolactive\b",
        r"\bprime 150\b",
        r"\bbluechip\b",
        r"\bdiversified international equity\b",
        r"\bglobal select fund\b",
        r"\blarge core\b",
        r"\bsmid core\b",
        r"\bblue chip\b",
        r"\btotal market\b",
        r"\bmarket neutral\b",
        r"\bbeyond china\b",
        r"\bchina magnificent seven\b",
        r"\bglobal hope\b",
        r"\bdomestic resilience\b",
        r"\bhigh profitability\b",
        r"\bdomestic etf\b",
        r"\bdefensive tilt\b",
        r"\bcyclical tilt\b",
        r"\bfuture leadership\b",
        r"\bglobal innovators?\b",
        r"\binnovators fund\b",
        r"\bgreater canada\b",
        r"\bsustainable world\b",
        r"\bglobal edge\b",
        r"\bwomen'?s leadership\b",
        r"\ball country world\b",
        r"\bportfolio(?:wy)? fiz\b",
        r"\bportfelowy\b",
        r"\bconcentrated international\b",
        r"\binternational\b",
        r"\baustralian moat income\b",
        r"\baustralian high conviction\b",
        r"\basian opportunities\b",
        r"\basia active\b",
        r"\bindia active\b",
        r"\bglobal small compan(?:y|ies)\b",
        r"\bglobal emerging companies\b",
        r"\bglobal select\b",
        r"\bglobal innovation\b",
        r"\btech leaders?\b",
        r"\bsmart etf\b",
        r"\bactive etf\b",
        r"\bcore etf\b",
        r"\bfocused opportunity\b",
        r"\bfocus etf\b",
        r"\bselect etf\b",
        r"\bfounder[- ]led\b",
        r"\bnatural monopoly\b",
        r"\bengineering the future\b",
        r"\bclimate leaders?\b",
        r"\bnet[- ]zero emissions pathway\b",
        r"\bopportunities etf\b",
        r"\bstrategy etf\b",
        r"\bleaders etf\b",
        r"\binnovation etf\b",
        r"\bdefined innovation\b",
        r"\btransformative china\b",
        r"\bchina dragons\b",
        r"\bchina tech\b",
        r"\bchina elec vehicle\b",
        r"\bchina electric vehicle\b",
        r"\bvisionary etf\b",
        r"\bendowment style\b",
        r"\bfrontier economic fund\b",
        r"\bcore select etf\b",
        r"\b500 etf\b",
        r"\brotation etf\b",
        r"\bsector rotation\b",
        r"\bsector navigator\b",
        r"\bglobal brands etf\b",
        r"\bapac ex jp sus eq\b",
        r"\bus eqt\b",
        r"\bgroup ETF\b",
        r"\bgroup\+\b",
        r"\bk100\b",
        r"\bkrx300\b",
        r"\bktop30\b",
        r"\b200tr\b",
        r"\b200esg\b",
        r"\bidiv\b",
        r"\bibov(?:espa)?\b",
        r"\ba[cç]oes\b",
        r"\bfundo de [ií]ndice\b",
        r"\bfdo de [ií]ndice\b",
        "\u6df1\u8bc1",
        "\u4e0a\u8bc1",
        "\u6caa\u6df1",
        "\u4e2d\u8bc1",
        "\u56fd\u8bc1",
        "\u53cc\u521b50",
        "\u6e2f\u80a1\u901a",
        "\uc561\ud2f0\ube0c",
        "\ud3ec\ucee4\uc2a4",
        "\uac00\uce58",
    ),
)


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_ticker_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_existing_classifier_update_keys(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return {
        (row["ticker"], row["exchange"])
        for row in rows
        if row.get("field") in CLASSIFIER_METADATA_FIELDS and row.get("reason", "").startswith(CLASSIFIER_REASON_PREFIX)
    }


def name_haystack(name: str) -> str:
    return f" {ascii_fold(name).lower()} {name.lower()} "


def classify_etf_category(name: str) -> tuple[str, str]:
    haystack = name_haystack(name)
    for category_rule in ETF_CATEGORY_RULES:
        if any(pattern.search(haystack) for pattern in category_rule.patterns):
            return category_rule.category, category_rule.name
    return "", ""


def evaluate_etf_row(row: dict[str, str]) -> dict[str, Any]:
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "category_update": "",
        "matched_rule": "",
    }
    if row["asset_type"] != "ETF":
        return {**base, "decision": "not_etf"}
    if (row.get("etf_category", "") or row.get("sector", "")).strip():
        return {**base, "decision": "already_has_category"}

    category, matched_rule = classify_etf_category(row["name"])
    if not category:
        return {**base, "decision": "no_rule_match"}
    return {**base, "category_update": category, "matched_rule": matched_rule, "decision": "accept"}


def verify_etf_categories(
    rows: list[dict[str, str]],
    *,
    exchanges: set[str],
    existing_classifier_update_keys: set[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    existing_classifier_update_keys = existing_classifier_update_keys or set()
    results: list[dict[str, Any]] = []
    for row in rows:
        key = (row["ticker"], row["exchange"])
        should_refresh_existing_classifier_update = key in existing_classifier_update_keys
        if row["exchange"] not in exchanges or row["asset_type"] != "ETF":
            continue
        if (row.get("etf_category", "") or row.get("sector", "")).strip() and not should_refresh_existing_classifier_update:
            continue
        candidate_row = (
            {**row, "sector": "", "etf_category": ""}
            if should_refresh_existing_classifier_update
            else row
        )
        results.append(evaluate_etf_row(candidate_row))
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "etf_category",
                "decision": "update",
                "proposed_value": result["category_update"],
                "confidence": "0.68",
                "reason": f"Deterministic ETF-name classifier mapped the product name to '{result['category_update']}' via rule '{result['matched_rule']}'. This is an etf_category fill, not a stock-sector assertion.",
            }
        )
    return updates


def prune_stale_classifier_updates(
    path: Path,
    updates: list[dict[str, str]],
    *,
    exchanges: set[str] | None = None,
) -> None:
    if not path.exists():
        return
    current_keys = {(update["ticker"], update["exchange"]) for update in updates}
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    kept_rows = [
        row
        for row in rows
        if not (
            row.get("field") in CLASSIFIER_METADATA_FIELDS
            and row.get("reason", "").startswith(CLASSIFIER_REASON_PREFIX)
            and (exchanges is None or row["exchange"] in exchanges)
            and ((row["ticker"], row["exchange"]) not in current_keys or row.get("field") != "etf_category")
        )
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=METADATA_UPDATE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(kept_rows)


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing ETF categories from deterministic product-name rules.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", help="Restrict to one or more internal exchanges.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = load_ticker_rows(args.tickers_csv)
    exchanges = set(args.exchange or {row["exchange"] for row in rows})
    existing_classifier_update_keys = (
        load_existing_classifier_update_keys(args.metadata_updates_csv) if args.apply else set()
    )
    results = verify_etf_categories(
        rows,
        exchanges=exchanges,
        existing_classifier_update_keys=existing_classifier_update_keys,
    )
    if args.offset:
        results = results[args.offset :]
    if args.limit is not None:
        results = results[: args.limit]
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    if args.apply:
        prune_stale_classifier_updates(args.metadata_updates_csv, updates, exchanges=exchanges)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "candidates": len(results),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "exchanges": sorted(exchanges),
                "accepted_category_updates": len(updates),
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "applied": args.apply,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
